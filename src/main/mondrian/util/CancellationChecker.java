/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2016-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.MondrianProperties;
import mondrian.server.Execution;

/**
 * Encapsulates cancel and timeouts checks
 *
 * @author Yury_Bakhmutski
 * @since Jan 18, 2016
 */
public class CancellationChecker {
    private final int interval;
    private final Execution execution;

    public CancellationChecker(Execution execution) {
        this.interval =
            MondrianProperties.instance().CheckCancelOrTimeoutInterval.get();
        this.execution = execution;
    }

    public void check(int iteration)
    {
        if (execution != null && interval > 0 && iteration % interval == 0)
        {
            synchronized (execution) {
                execution.checkCancelOrTimeout();
            }
        }
    }
}
// End CancellationChecker.java
