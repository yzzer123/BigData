package com.yzzer.mr.writablecomparable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Hadoop的比较器对象，用于自定义Writable的比较规则
 */
public class FlowBeanComparator extends WritableComparator {

    public FlowBeanComparator() {
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBean abean = (FlowBean) a;
        FlowBean bbean = (FlowBean) b;

        return abean.getSumFlow().compareTo(bbean.getSumFlow());
    }
}
