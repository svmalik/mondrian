/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2015 Pentaho and others
// All Rights Reserved.
 */
package mondrian.olap;

import mondrian.mdx.*;

import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapMemberBase;
import mondrian.util.IdentifierParser;
import org.apache.commons.collections.*;
import org.apache.log4j.Logger;
import org.olap4j.mdx.IdentifierSegment;

import java.util.*;

import static org.apache.commons.collections.CollectionUtils.filter;

/**
 * Used to collect and resolve identifiers in groups of children
 * where possible.  For example, if an enumerated set within an MDX
 * query includes references to 10 stores under the parent
 *
 *   [USA].[CA].[San Francisco]
 *
 * the class will attempt to identify those 10 identifiers
 * and issue a single lookup, resulting in fewer and more efficient
 * SQL queries.
 * The resulting collection of resolved identifiers is returned in a
 * map of <QueryPart, QueryPart>, where the unresolved Exp object acts
 * as the key.
 *
 * This class makes no assurances that all identifiers will be resolved.
 * The map returned by .resolve() will include only those identifiers
 * successfully resolved.
 *
 */
public final class IdBatchResolver {
    static final Logger LOGGER = Logger.getLogger(IdBatchResolver.class);

    private final Query query;

    // dimension and hierarchy unique names are collected during init
    // to assist in classifying Ids as potentially resolvable to members.
    private final Collection<String> dimensionUniqueNames =
        new ArrayList<String>();
    private final Collection<String> hierarchyUniqueNames =
        new ArrayList<String>();
    // level names are checked against the identifiers to avoid incorrectly
    // interpreting a Dimension.Level reference as Dimension.Member.
    private final Collection<String> levelNames =
        new ArrayList<String>();

    // Set of identifiers, sorted via IdComparator, which orders based
    // first on segment length (shortest to longest), then alphabetically.
    private  SortedSet<Id> identifiers = new TreeSet<Id>(new IdComparator());

    private final boolean ssasCompatibleNaming =
        MondrianProperties.instance().SsasCompatibleNaming.get();

    public IdBatchResolver(Query query) {
        this.query = query;
        initOlapElementNames(query.getCube());
        initIdentifiers(query);
    }

    public List<Member> resolveMembers(String string) {
        List<List<IdentifierSegment>> identifierList =
            IdentifierParser.parseIdentifierList(string);
        List<Member> members = new ArrayList<>(identifierList.size());
        if (!identifierList.isEmpty()) {
            Set<Id> membersIds = new LinkedHashSet<>(identifierList.size());
            for (List<IdentifierSegment> segments : identifierList) {
                Id id = new Id(Util.convert(segments));
                this.identifiers.add(id);
                membersIds.add(id);
            }
            expandIdentifiers(this.identifiers);
            Map<QueryPart, QueryPart> membersMap =
                resolveInParentGroupings(this.identifiers);
            for (Id id : membersIds) {
                QueryPart q = membersMap.get(id);
                if (q != null && q instanceof MemberExpr) {
                    members.add(((MemberExpr) q).getMember());
                }
            }
        }
        return members;
    }

    /**
     * Initializes the dimensionUniqueNames, hierarchyUniqueNames and
     * levelNames collections based on the contents of cube.  These collections
     * will be used to help determine whether identifiers correspond to
     * a dimension/hierarchy/level.
     */
    private void initOlapElementNames(Cube cube) {
        dimensionUniqueNames.addAll(
            getOlapElementNames(cube.getDimensions(), true));
        for (Dimension dim : cube.getDimensions()) {
            hierarchyUniqueNames.addAll(
                getOlapElementNames(dim.getHierarchies(), true));
            for (Hierarchy hier : dim.getHierarchies()) {
                levelNames.addAll(getOlapElementNames(hier.getLevels(), false));
            }
        }
    }

    /**
     * Initializes the identifiers collection by walking the axes
     * and formulas in the query and adding each encountered Id.
     * Finally, expands the set of identifiers to include parents.  E.g.
     * if the identifier
     *   [Store].[All Store].[USA].[CA]
     * is encountered, this will be expanded to include
     *   [Store].[All Store].[USA]
     *   [Store].[All Store]
     */
    private void initIdentifiers(Query query) {
        MdxVisitor identifierVisitor = new IdentifierVisitor(identifiers);
        QueryAxis[] axes = query.getAxes();
        Formula[] formulas = query.getFormulas();
        for (QueryAxis axis : axes) {
            axis.accept(identifierVisitor);
        }
        if (query.getSlicerAxis() != null) {
            query.getSlicerAxis().accept(identifierVisitor);
        }
        for (Formula formula : formulas) {
            formula.accept(identifierVisitor);
        }
        expandIdentifiers(identifiers);
    }

    /**
     * Attempts to resolve the identifiers contained in the query in
     * batches based on the parent, e.g. looking up and resolving the
     * states in the set:
     *   { [Store].[USA].[CA], [Store].[USA].[OR] }
     * together rather than individually.
     * Note that there is no guarantee that all identifiers will be
     * resolved.  Calculated members, for example, are explicitly not
     * handled here.  The purpose of this class is to improve efficiency
     * of resolution of non-calculated members, but must be followed
     * by more thorough expression resolution.
     *
     * @return  a Map of the expressions Id elements mapped to their
     * respective resolved Exp.
     */
    public Map<QueryPart, QueryPart> resolve() {
        return resolveInParentGroupings(identifiers);
    }

    /**
     *  Loops through the SortedSet of Ids, attempting to load sets of
     *  children of parent Ids.
     *  The loop below assumes the the SortedSet is ordered by segment
     *  size from smallest to largest, such that parent identifiers will
     *  occur before their children.
     */
    private  Map<QueryPart, QueryPart> resolveInParentGroupings(
        SortedSet<Id> identifiers)
    {
        final Map<QueryPart, QueryPart> resolvedIdentifiers =
            new LinkedHashMap<>();

        while (identifiers.size() > 0) {
            Id parent = identifiers.first();
            identifiers.remove(parent);

            if (!supportedIdentifier(parent, true, true) && !supportedIdentifierKey(parent, true)) {
                continue;
            }
            Exp exp = (Exp)resolvedIdentifiers.get(parent);
            if (exp == null) {
                exp = lookupExp(resolvedIdentifiers, parent);
            }
            Member parentMember = getMemberFromExp(exp);
            if (supportedMember(parentMember)) {
                if (getLastSegment(parent) instanceof Id.NameSegment) {
                    batchResolveChildren(
                        parent, parentMember, identifiers, resolvedIdentifiers);
                } else if (getLastSegment(parent) instanceof Id.KeySegment) {
                    batchResolveChildrenByKey(
                        parent, parentMember, parentMember.getLevel(), identifiers, resolvedIdentifiers);
                }
            }
            if (exp instanceof LevelExpr) {
                Level level = ((LevelExpr)exp).getLevel();
                batchResolveChildrenByKey(
                    parent, null, level, identifiers, resolvedIdentifiers);
            }
        }
        return resolvedIdentifiers;
    }

    /**
     * Find the children of Id parent in the identifiers set and resolves
     * all supported children together, adding them to the resolvedIdentifiers
     * map.
     */
    private void batchResolveChildren(
        Id parent, Member parentMember, SortedSet<Id> identifiers,
        Map<QueryPart, QueryPart> resolvedIdentifiers)
    {
        final List<Id> children = findChildIds(parent, identifiers);
        if (children.size() == 0) return;
        final List<Id.NameSegment> childNameSegments =
            collectChildrenNameSegments(parentMember, children);

        if (childNameSegments.size() > 0) {
            List<Member> childMembers =
                lookupChildrenByNames(parentMember, childNameSegments);
            addChildrenToResolvedMap(
                resolvedIdentifiers, children, childMembers);
        }
    }

    private Exp lookupExp(
        Map<QueryPart, QueryPart> resolvedIdentifiers, Id parent)
    {
        try {
            Exp exp = Util.lookup(query, parent.getSegments(), false);
            resolvedIdentifiers.put(parent, (QueryPart)exp);
            return exp;
        } catch (Exception exception) {
            LOGGER.info(
                String.format(
                    "Failed to resolve '%s' during batch ID "
                    + "resolution.",
                    parent));
        }
        return null;
    }

    /**
     * Correlates each child Id we started with to it's associated
     * Member, if present.  Updates the resolvedIdentifiers map with
     * the association.
     */
    private void addChildrenToResolvedMap(
        Map<QueryPart, QueryPart> resolvedIdentifiers, List<Id> children,
        List<Member> childMembers)
    {
        for (Member child : childMembers) {
            for (Id childId : children) {
                if (!resolvedIdentifiers.containsKey(childId))
                {
                    Id.Segment segment = getLastSegment(childId);
                    if (areEqualKeys(segment, child))
                    {
                        resolvedIdentifiers.put(
                            childId, (QueryPart) Util.createExpr(child));
                    }
                }
            }
        }
    }

    private boolean areEqualKeys(Id.Segment segment, Member member) {
        if (segment instanceof Id.NameSegment) {
            return segment.matches(member.getName());
        }
        if (segment instanceof Id.KeySegment && member instanceof RolapMember) {
            return segment.getKeyParts().get(0).getName().equals(
                RolapMemberBase.keyToString(((RolapMember)member).getKey()));
        }
        return false;
    }

    /**
     * Performs a lookup of a set of children under parentMember.
     */
    private List<Member> lookupChildrenByNames(
        Member parentMember,
        List<Id.NameSegment> childNameSegments)
    {
        try {
            return query.getSchemaReader(true)
                .lookupMemberChildrenByNames(
                    parentMember,
                    childNameSegments, MatchType.EXACT);
        } catch (Exception e) {
            LOGGER.info(
                String.format(
                    "Failure while looking up children of '%s' during  "
                    + "batch member resolution.  Child member refs:  %s",
                    parentMember,
                    Arrays.toString(childNameSegments.toArray())), e);
        }
        // don't want to fail at this point.  Member resolution still has
        // another chance to succeed.
        return Collections.emptyList();
    }

    /**
     * Filters the children list to those that contain identifiers
     * we think we can batch resolve, then transforms the Id list
     * to the corresponding NameSegment.
     */
    private List<Id.NameSegment> collectChildrenNameSegments(
        final Member parentMember, List<Id> children)
    {
        filter(
            children, new Predicate() {
            // remove children we can't support
                public boolean evaluate(Object theId)
                {
                    Id id = (Id)theId;
                    return !Util.matches(parentMember, id.getSegments())
                        && supportedIdentifier(id, false, false);
                }
            });
        return new ArrayList(
            CollectionUtils.collect(
                children, new Transformer()
            {
                // convert the collection to a list of NameSegments
            public Object transform(Object theId) {
                Id id = (Id)theId;
                return getLastSegment(id);
            }
        }));
    }

    private Id.Segment getLastSegment(Id id) {
        int segSize = id.getSegments().size();
        return id.getSegments().get(segSize - 1);
    }

    /**
     * Checks various conditions to determine whether
     * the given identifier is likely to be resolvable at this point.
     */
    private boolean supportedIdentifier(Id id, boolean acceptLevel, boolean acceptHierarchy) {
        Id.Segment seg = getLastSegment(id);
        if (!(seg instanceof Id.NameSegment)) {
            // we can't batch resolve members identified by key
            return false;
        }
        return isPossibleMemberRef(id, acceptLevel, acceptHierarchy)
            && !segmentIsCalcMember(id.getSegments())
            && !id.getSegments().get(0).matches("Measures");
    }

    private void batchResolveChildrenByKey(
        Id parent, Member parentMember, Level level, SortedSet<Id> identifiers,
        Map<QueryPart, QueryPart> resolvedIdentifiers)
    {
        final List<Id> children = findChildIds(parent, identifiers);
        if (children.size() == 0) return;
        final List<Id.KeySegment> childKeySegments =
            collectChildrenKeySegments(parentMember, children);
        if (childKeySegments.size() > 0) {
            List<Member> childMembers =
                lookupChildrenByKeys(level, childKeySegments);
            addChildrenToResolvedMap(
                resolvedIdentifiers, children, childMembers);
        }
    }

    private List<Member> lookupChildrenByKeys(
        Level level,
        List<Id.KeySegment> childKeys)
    {
        try {
            return query.getSchemaReader(true)
                .getLevelMembers(level, childKeys, MatchType.EXACT);
        } catch (Exception e) {
            LOGGER.info(
                String.format(
                    "Failure while looking up members of '%s' during  "
                    + "batch member resolution.  Child member refs:  %s",
                    level,
                    Arrays.toString(childKeys.toArray())), e);
        }
        // don't want to fail at this point.  Member resolution still has
        // another chance to succeed.
        return Collections.emptyList();
    }

    private List<Id.KeySegment> collectChildrenKeySegments(
        final Member parentMember, List<Id> children)
    {
        filter(
            children, new Predicate() {
                // remove children we can't support
                public boolean evaluate(Object theId)
                {
                    Id id = (Id)theId;
                    return (parentMember == null ||
                            !Util.matches(parentMember, id.getSegments()))
                        && supportedIdentifierKey(id, false);
                }
            });
        return new ArrayList(
            CollectionUtils.collect(
                children, new Transformer()
                {
                    // convert the collection to a list of NameSegments
                    public Object transform(Object theId) {
                        Id id = (Id)theId;
                        return getLastSegment(id);
                    }
                }));
    }

    private boolean supportedIdentifierKey(Id id, boolean acceptHierarchy) {
        Id.Segment seg = getLastSegment(id);
        if (!(seg instanceof Id.KeySegment)) {
            return false;
        }
        return isPossibleMemberRef(id, true, acceptHierarchy)
            && !segmentIsCalcMember(id.getSegments())
            && !id.getSegments().get(0).matches("Measures");
    }

    private boolean supportedMember(Member member) {
        return !(member == null
            || member.equals(
                member.getHierarchy().getNullMember())
            || member.isMeasure());
    }

    /**
     * Returns the [All] member from HierarchyExpr and DimensionExpr
     * associated with hierarchies that have an All member.
     * Returns the member associated with a MemberExpr.
     * For all other Exp returns null.
     */
    private Member getMemberFromExp(Exp exp) {
        if (exp instanceof DimensionExpr) {
            Hierarchy hier = ((DimensionExpr)exp)
                .getDimension().getHierarchy();
            if (hier.hasAll()) {
                return hier.getAllMember();
            }
        } else if (exp instanceof HierarchyExpr) {
            Hierarchy hier = ((HierarchyExpr)exp)
                .getHierarchy();
            if (hier.hasAll()) {
                return hier.getAllMember();
            }
        } else if (exp instanceof MemberExpr) {
            return ((MemberExpr)exp).getMember();
        }
        return null;
    }

    /**
     * Returns a collection of strings corresponding to the name
     * or uniqueName of each OlapElement in olapElements, based on the
     * flag uniqueName.
     */
    private Collection<String> getOlapElementNames(
        OlapElement[] olapElements, final boolean uniqueName)
    {
        return CollectionUtils.collect(
            Arrays.asList(olapElements),
            new Transformer() {
                public Object transform(Object o) {
                    return uniqueName ? ((OlapElement)o).getUniqueName()
                        : ((OlapElement)o).getName();
                }
            });
    }

    /**
     * Returns true if the Id is something that will potentially translate into
     * either the All/Default member of a dimension/hierarchy,
     * or a specific member.
     * This filters out references that we'd be unlikely to effectively
     * handle.
     */
    private boolean isPossibleMemberRef(Id id, boolean acceptLevel, boolean acceptHierarchy) {
        int size = id.getSegments().size();

        if (size == 1) {
            //Id.Segment seg = id.getSegments().get(0);
            return segListMatchInUniqueNames(
                id.getSegments(), dimensionUniqueNames)
                || (segListMatchInUniqueNames(
                    id.getSegments(), hierarchyUniqueNames) && acceptHierarchy);
        }
        if (ssasCompatibleNaming && size == 2)
        {
            return segListMatchInUniqueNames(
                id.getSegments(), hierarchyUniqueNames) && acceptHierarchy;
        }
        if (!acceptLevel && segMatchInNames(getLastSegment(id), levelNames)) {
            // conservative.  false on any match of any level name
            return false;
        }
        // don't support "shortcut" member references references
        return size > 1;
    }

    private boolean segListMatchInUniqueNames(
        List<Id.Segment> segments, Collection<String> names)
    {
        String segUniqueName = Util.implode(segments);
        for (String name : names) {
           if (Util.equalName(segUniqueName, name)) {
               return true;
           }
        }
        return false;
    }

    private boolean segMatchInNames(
        Id.Segment seg, Collection<String> names)
    {
        for (String name : names) {
            if (seg.matches(name)) {
                return true;
            }
        }
        return false;
    }

    private boolean segmentIsCalcMember(final List<Id.Segment> checkSegments) {
        return query.getSchemaReader(true)
            .getCalculatedMember(checkSegments) != null;
    }

    private List<Id> findChildIds(Id parent, SortedSet<Id> identifiers) {
        List<Id> childIds = new ArrayList<Id>();
        for (Id id : identifiers) {
            final List<Id.Segment> idSeg = id.getSegments();
            final List<Id.Segment> parentSegments = parent.getSegments();
            final int parentSegSize = parentSegments.size();
            if (idSeg.size() == parentSegSize + 1
                && parent.getSegments().equals(
                    idSeg.subList(0, parentSegSize)))
            {
                childIds.add(id);
            }
        }
        return childIds;
    }

    /**
     * Adds each parent segment to the set.
     */
    private void expandIdentifiers(Set<Id> identifiers) {
        Set<Id> expandedIdentifiers = new HashSet<Id>();
        for (Id id : identifiers) {
            for (int i = 1; i < id.getSegments().size(); i++) {
                expandedIdentifiers.add(new Id(id.getSegments().subList(0, i)));
            }
        }
        identifiers.addAll(expandedIdentifiers);
    }

    /**
     * Sorts shorter segments first, then by string compare.
     * This allows processing parents first during the lookup loop,
     * which is required by the algorithm.
     */
    private static class IdComparator implements Comparator<Id> {
        public int compare(Id o1, Id o2) {
            List<Id.Segment> o1Seg = o1.getSegments();
            List<Id.Segment> o2Seg = o2.getSegments();

            if (o1Seg.size() > o2Seg.size()) {
                return 1;
            } else if (o1Seg.size() < o2Seg.size()) {
                return -1;
            } else {
                return o1Seg.toString()
                    .compareTo(o2Seg.toString());
            }
        }
    }
}
// End IdBatchResolver.java
