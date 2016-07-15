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

package mondrian.spi.impl;

import java.util.ArrayList;
import java.util.List;

import mondrian.olap.MondrianServer;
import mondrian.olap.Role;
import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapSchema;
import mondrian.spi.RoleLoader;
import mondrian.util.LockBox;

/**
 * Default implementation of the RoleLoader interface.
 * This uses the Role connection property to load roles
 * from the rolap schema.
 */
public class RoleLoaderImpl implements RoleLoader {

    /**
     * Load roles. This uses the 'Role' connection
     * property of the connect info.
     * The list of roles is never null, but will be empty if no roles
     * are defined using the Role connection property.
     * If a defined roles name cannot be found, an error is
     * thrown.
     *
     * @param server      the Mondrian server instance
     * @param schema      the rolap schema associated with the rolap connection
     * @param connectInfo the connection info
     * @return a list of roles to apply to the connection.
     */
    @Override
    public List<Role> loadRoles(MondrianServer server, RolapSchema schema,
                                Util.PropertyList connectInfo) {
        List<Role> roleList = new ArrayList<Role>();
        String roleNameList = connectInfo.get(RolapConnectionProperties.Role.name());
        if (roleNameList != null) {
            List<String> roleNames = Util.parseCommaList(roleNameList);
            for (String roleName : roleNames) {
                Role role1 = null;
                final LockBox.Entry entry = server.getLockBox()
                                                  .get(roleName);
                if (entry != null) {
                    try {
                        role1 = (Role) entry.getValue();
                    } catch (ClassCastException e) {
                        role1 = null;
                    }
                }
                if (role1 == null) {
                    role1 = schema.lookupRole(roleName);
                }
                if (role1 == null) {
                    throw Util.newError("Role '" + roleName + "' not found");
                }
                roleList.add(role1);
            }
        }
        return roleList;
    }

}
