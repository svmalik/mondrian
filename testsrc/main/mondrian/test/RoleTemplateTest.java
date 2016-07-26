/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2004-2005 TONBELLER AG
 * Copyright (C) 2005-2005 Julian Hyde
 * Copyright (C) 2005-2015 Pentaho and others
 * All Rights Reserved.
 */

package mondrian.test;

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import mondrian.olap.Access;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianServer;
import mondrian.olap.Role;
import mondrian.olap.Schema;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapSchema;
import mondrian.spi.impl.RoleLoaderImpl;

/**
 *
 */
public class RoleTemplateTest extends FoodMartTestCase {

    public RoleTemplateTest(String name) {
        super(name);
        propSaver.set(MondrianProperties.instance().RoleLoader,
            CustomDataRoleLoader.class.getName());
    }

    private void assertMemberAccess(final Connection connection, Access expectedAccess,
                                    String memberName) {
        final Role role = connection.getRole(); // restricted
        Schema schema = connection.getSchema();
        final boolean fail = true;
        Cube salesCube = schema.lookupCube("Sales", fail);
        final SchemaReader schemaReader = salesCube.getSchemaReader(null)
                                                   .withLocus();
        final Member member = schemaReader.getMemberByUniqueName(Util.parseIdentifier(memberName),
            true);
        final Access actualAccess = role.getAccess(member);
        Assert.assertEquals(memberName, expectedAccess, actualAccess);
    }

    private String getTemplate(String dependsOnRole) {
        return "<RoleTemplate name=\"LA Member Role\""
            + (dependsOnRole != null ? " dependsOnRole=\"" + dependsOnRole + "\"" : "")
            + ">\n"
            + "<![CDATA[\n"
            + "<Role name=\"$member\">\n"
            + "  <SchemaGrant access=\"none\">\n"
            + "    <CubeGrant cube=\"Sales\" access=\"all\">\n"
            + "      <HierarchyGrant hierarchy=\"[Store]\" access=\"custom\" topLevel=\"[Store].[Store Country]\">\n"
            + "        <MemberGrant member=\"[Store].[USA]\" access=\"none\"/>\n"
            + "        <MemberGrant member=\"[Store].[USA].[CA].[$member]\" access=\"all\"/>\n"
            + "      </HierarchyGrant>\n"
            + "    </CubeGrant>\n"
            + "  </SchemaGrant>\n"
            + "</Role>\n"
            + "]]>\n"
            + "</RoleTemplate>";
    }

    public final TestContext withRoleTemplates(TestContext testContext, final String templateDefs) {
        String schema = TestContext.getRawFoodMartSchema();
        // Put role template at the end
        if (templateDefs != null) {
            int i = schema.indexOf("</Schema>");
            schema = schema.substring(0, i) + templateDefs + schema.substring(i);
        }
        return testContext.withSchema(schema);
    }

    public void testRoleMemberAccessWithRoleTemplate() {
        TestContext context = withRoleTemplates(getTestContext(), getTemplate(null));
        Util.PropertyList properties = context.getConnectionProperties()
                                              .clone();
        properties.put("CustomData", "Los Angeles");
        context = context.withProperties(properties)
                         .withFreshConnection();
        final Connection connection = context.getConnection();
        // because CA has access
        assertMemberAccess(connection, Access.CUSTOM, "[Store].[USA]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF].[Mexico City]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada].[BC].[Vancouver]");
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[Los Angeles]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[CA].[San Diego]");
        // USA deny supercedes OR grant
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[OR].[Portland]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA].[Seattle]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA]");
        // above top level
        assertMemberAccess(connection, Access.NONE, "[Store].[All Stores]");
    }

    public void testRoleMemberAccessWithLazyLoadRoleTemplate() {
        propSaver.set(propSaver.properties.LazyLoadRoles, true);
        testRoleMemberAccessWithRoleTemplate();
    }

    public void testRoleMemberAccessWithRoleTemplateWithDependency() {
        String role = "California manager";
        TestContext context = withRoleTemplates(
            getTestContext().withRole(role),
            getTemplate(role));
        Util.PropertyList properties =
            context.getConnectionProperties().clone();
        properties.put("CustomData", "Los Angeles");
        final Connection connection =
            context.withProperties(properties)
                .withFreshConnection().getConnection();
        // because CA has access
        assertMemberAccess(connection, Access.CUSTOM, "[Store].[USA]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF].[Mexico City]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada].[BC].[Vancouver]");
        // has access due to custom role
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[Los Angeles]");
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[San Diego]");
        // USA deny supercedes OR grant
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[OR].[Portland]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA].[Seattle]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA]");
        // above top level
        assertMemberAccess(connection, Access.NONE, "[Store].[All Stores]");
    }

    public void testRoleMemberAccessWithRoleTemplateWithDependency2() {
        TestContext context = withRoleTemplates(
            getTestContext().withRole("California manager"),
            getTemplate("California manager2"));
        Util.PropertyList properties =
            context.getConnectionProperties().clone();
        properties.put("CustomData", "Los Angeles");
        final Connection connection =
            context.withProperties(properties)
                .withFreshConnection().getConnection();
        // because CA has access
        assertMemberAccess(connection, Access.CUSTOM, "[Store].[USA]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Mexico].[DF].[Mexico City]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada]");
        assertMemberAccess(connection, Access.NONE, "[Store].[Canada].[BC].[Vancouver]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[CA].[Los Angeles]");
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[San Diego]");
        // USA deny supercedes OR grant
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[OR].[Portland]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA].[Seattle]");
        assertMemberAccess(connection, Access.NONE, "[Store].[USA].[WA]");
        // above top level
        assertMemberAccess(connection, Access.NONE, "[Store].[All Stores]");
    }

    public void testRoleMemberAccessWithRoleTemplateWithDependencyNoRestrictions() {
        String role = "California manager";
        TestContext context = withRoleTemplates(
            getTestContext(),
            getTemplate(role));
        Util.PropertyList properties =
            context.getConnectionProperties().clone();
        properties.put("CustomData", "Los Angeles");
        final Connection connection =
            context.withProperties(properties)
                .withFreshConnection().getConnection();
        // custom role should not apply here due to dependency
        assertMemberAccess(connection, Access.ALL, "[Store].[USA]");
        assertMemberAccess(connection, Access.ALL, "[Store].[USA].[CA].[Los Angeles]");
    }

    public static class CustomDataRoleLoader extends RoleLoaderImpl {
        public List<Role> loadRoles(MondrianServer server, RolapSchema schema,
                                    Util.PropertyList connectInfo) {
            List<Role> roles = super.loadRoles(server, schema, connectInfo);
            String customData = connectInfo.get("CustomData");
            if (customData != null) {
                List<String> ids = Util.parseCommaList(customData);
                MondrianDef.RoleTemplate[] templates = schema.getXMLSchema().roleTemplates;
                for (MondrianDef.RoleTemplate template : templates) {
                    String name = template.name;
                    if (name.equalsIgnoreCase("LA Member Role")) {
                        String parentRoles = template.dependsOnRole;
                        if (parentRoles != null && !parentRoles.isEmpty()) {
                            List<String> parentRolesList = Util.parseCommaList(parentRoles);
                            String currentRoles = connectInfo.get(RolapConnectionProperties.Role.name());
                            if (currentRoles == null || currentRoles.isEmpty()) {
                                continue;
                            }
                            List<String> currentRolesList = Util.parseCommaList(currentRoles);
                            if (Collections.disjoint(parentRolesList, currentRolesList)) {
                                continue;
                            }
                        }

                        String xml = template.cdata;
                        for (String id : ids) {
                            id = id.trim();
                            if (id.length() > 0) {
                                String mod = xml.replace("$member", id);
                                String roleId = schema.addRole(mod);
                                Role created = schema.lookupRole(roleId);
                                roles.add(created);
                            }
                        }
                    }
                }
            }
            return roles;
        }
    }
}
