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

package mondrian.spi;

import java.util.List;

import mondrian.olap.MondrianServer;
import mondrian.olap.Role;
import mondrian.olap.Util;
import mondrian.rolap.RolapSchema;

/**
 * Interface for getting roles for a RolapConnection.
 */
public interface RoleLoader {

    /**
     * Load roles for a rolap connection. Various strategies can be used.
     * In general, the 'Role' connection
     * property should at least form the basis of the roles supplied. This can return
     * null or an empty list, if no roles can be found.
     * @param server the Mondrian server instance
     * @param schema the rolap schema associated with the rolap connection
     * @param connectInfo the connection info
     * @return a list of roles to apply to the connection.
     */
    public List<Role> loadRoles(MondrianServer server, RolapSchema schema,
                                Util.PropertyList connectInfo);

}
