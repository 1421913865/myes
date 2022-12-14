package com.example.myes.event;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * @author <a href="mailto:wangchao.star@gmail.com">wangchao</a>
 * @version 1.0
 * @since 2017-08-26 22:30:00
 */
public class UpdateAbstractCanalEvent extends AbstractCanalEvent {
    /**
     * Create a new ApplicationEvent.
     *
     * @param source the object on which the event initially occurred (never {@code null})
     */
    public UpdateAbstractCanalEvent(Entry source) {
        super(source);
    }
}
