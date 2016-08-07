package com.bloom.runtime.window;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger; // Referenced classes of package
								// com.bloom.runtime.window: //
								// SlidingPolicy, BufWindowSub, CmpAttrs,
								// WindowPolicy, // AttrExtractor, BufWindow,
								// BufferManager public abstract class
								// BufWindowFactory { private
								// BufWindowFactory(WindowPolicy wp) { id =
								// next_window_id.getAndIncrement(); policy =
								// wp; } public SlidingPolicy.OutputBuffer
								// createOutputBuffer(BufWindow wnd) {
								// SlidingPolicy slidingPolicy =
								// policy.getSlidingPolicy(); return
								// slidingPolicy != null ?
								// slidingPolicy.createOutputBuffer(wnd) : null;
								// } public WindowPolicy getPolicy() { return
								// policy; } public void addSub(BufWindowSub
								// sub) { this.sub = sub; } public void
								// removeAllSubsribers() { sub = null; } public
								// abstract BufWindow createWindow(BufferManager
								// buffermanager); public string toString() {
								// return (new
								// StringBuilder()).append("bufwindow(").append(id).append(",
								// ").append(getClass().getCanonicalName()).append(",
								// ").append(policy).append(")").toString(); }
								// public int hashCode() { return id; } public
								// boolean equals(object o) { return (o
								// instanceof BufWindowFactory) &&
								// ((BufWindowFactory)o).id == id; } void
								// receive(RecordKey partKey, collection
								// snapshot, IBatch newEntries, IBatch
								// oldEntries) { if(!$assertionsDisabled &&
								// oldEntries.size() + newEntries.size() <= 0)
								// throw new AssertionError(); BufWindowSub s =
								// sub; if(s != null) s.receive(partKey,
								// snapshot, newEntries, oldEntries); } private
								// static BufWindowFactory create(WindowPolicy
								// wp) { /* anonymous class not found */ class
								// _anm11 {}
								// switch(_cls11..SwitchMap.com.bloom.runtime.meta.IntervalPolicy.Kind[wp.getKind().ordinal()])
								// { case 1: // '\001' if(wp.isJumping()) return
								// new object(wp) /* anonymous class not found
								// */ class _anm1 {} ; else return new
								// object(wp) /* anonymous class not found */
								// class _anm2 {} ; case 2: // '\002'
								// if(wp.isJumping()) return new object(wp) /*
								// anonymous class not found */ class _anm3 {} ;
								// else return new object(wp) /* anonymous class
								// not found */ class _anm4 {} ; case 3: //
								// '\003' if(wp.isJumping()) return new
								// object(wp) /* anonymous class not found */
								// class _anm5 {} ; else return new object(wp)
								// /* anonymous class not found */ class _anm6
								// {} ; case 4: // '\004' if(wp.isJumping())
								// return new object(wp) /* anonymous class not
								// found */ class _anm7 {} ; else return new
								// object(wp) /* anonymous class not found */
								// class _anm8 {} ; case 5: // '\005'
								// if(wp.isJumping()) return new object(wp) /*
								// anonymous class not found */ class _anm9 {} ;
								// else return new object(wp) /* anonymous class
								// not found */ class _anm10 {} ; } throw new
								// illegalargumentexception((new
								// StringBuilder()).append("unknow kind of
								// window ").append(wp.getKind()).toString()); }
								// public static BufWindowFactory
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind
								// kind, boolean isJumping, integer rowCount,
								// CmpAttrs attrComparator, long timeInterval,
								// SlidingPolicy slidingPolicy) { WindowPolicy
								// wp = WindowPolicy.makePolicy(kind, isJumping,
								// rowCount, attrComparator, timeInterval,
								// slidingPolicy); return create(wp); } private
								// static SlidingPolicy
								// createSlidingPolicy(com.bloom.runtime.meta.MetaInfo.window
								// wi, BaseServer srv) { if(wi.slidePolicy ==
								// null) { return null; } else { IntervalPolicy
								// ip = wi.windowLen; IntervalPolicy sp =
								// wi.slidePolicy;
								// com.bloom.runtime.meta.IntervalPolicy.Kind
								// kind = sp.getKind();
								// com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy
								// iap = ip.getAttrPolicy();
								// com.bloom.runtime.meta.IntervalPolicy.AttrBasedPolicy
								// isp = sp.getAttrPolicy(); CmpAttrs
								// sAttrComparator = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.ATTR
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR
								// ? null :
								// AttrExtractor.createAttrComparator(iap.getAttrName(),
								// isp.getAttrValueRange() / 1000L, wi, srv);
								// integer sRowCount = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.COUNT
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_COUNT
								// ? null :
								// integer.valueOf(sp.getCountPolicy().getCountInterval());
								// long sTimeInterval = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR
								// ? null :
								// long.valueOf(TimeUnit.MICROSECONDS.toNanos(sp.getTimePolicy().getTimeInterval()));
								// return SlidingPolicy.create(kind, sRowCount,
								// sAttrComparator, sTimeInterval); } } public
								// static BufWindowFactory
								// create(com.bloom.runtime.meta.MetaInfo.window
								// wi, BaseServer srv, BufWindowSub sub) {
								// IntervalPolicy ip = wi.windowLen; boolean
								// isJumping = wi.jumping;
								// com.bloom.runtime.meta.IntervalPolicy.Kind
								// kind = ip.getKind(); CmpAttrs attrComparator
								// = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.ATTR
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR
								// ? null :
								// AttrExtractor.createAttrComparator(wi, srv);
								// integer rowCount = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.COUNT
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_COUNT
								// ? null :
								// integer.valueOf(ip.getCountPolicy().getCountInterval());
								// long timeInterval = kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR
								// && kind !=
								// com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_COUNT
								// ? null :
								// long.valueOf(TimeUnit.MICROSECONDS.toNanos(ip.getTimePolicy().getTimeInterval()));
								// SlidingPolicy slidingPolicy =
								// createSlidingPolicy(wi, srv);
								// BufWindowFactory fac = create(kind,
								// isJumping, rowCount, attrComparator,
								// timeInterval, slidingPolicy);
								// fac.addSub(sub); return fac; } public static
								// BufWindowFactory createSlidingCount(int
								// row_count) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.COUNT,
								// false, integer.valueOf(row_count), null,
								// null, null); } public static BufWindowFactory
								// createSlidingAttribute(CmpAttrs
								// attrComparator) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.ATTR,
								// false, null, attrComparator, null, null); }
								// public static BufWindowFactory
								// createSlidingTime(long time_interval) {
								// return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME,
								// false, null, null,
								// long.valueOf(time_interval), null); } public
								// static BufWindowFactory
								// createSlidingTimeCount(long time_interval,
								// int row_count) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_COUNT,
								// false, integer.valueOf(row_count), null,
								// long.valueOf(time_interval), null); } public
								// static BufWindowFactory
								// createSlidingTimeAttr(long time_interval,
								// CmpAttrs attrComparator) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR,
								// false, null, attrComparator,
								// long.valueOf(time_interval), null); } public
								// static BufWindowFactory
								// createJumpingCount(int row_count) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.COUNT,
								// true, integer.valueOf(row_count), null, null,
								// null); } public static BufWindowFactory
								// createJumpingAttribute(CmpAttrs
								// attrComparator) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.ATTR,
								// true, null, attrComparator, null, null); }
								// public static BufWindowFactory
								// createJumpingTime(long time_interval) {
								// return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME,
								// true, null, null,
								// long.valueOf(time_interval), null); } public
								// static BufWindowFactory
								// createJumpingTimeCount(long time_interval,
								// int row_count) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_COUNT,
								// true, integer.valueOf(row_count), null,
								// long.valueOf(time_interval), null); } public
								// static BufWindowFactory
								// createJumpingTimeAttr(long time_interval,
								// CmpAttrs attrComparator) { return
								// create(com.bloom.runtime.meta.IntervalPolicy.Kind.TIME_ATTR,
								// true, null, attrComparator,
								// long.valueOf(time_interval), null); }
								// BufWindowFactory(WindowPolicy x0, _cls1 x1) {
								// this(x0); } protected static Logger logger =
								// Logger.getLogger(com/bloom/runtime/window/BufWindowFactory);
								// private static AtomicInteger next_window_id =
								// new AtomicInteger(1); protected final int id;
								// private volatile BufWindowSub sub; private
								// final WindowPolicy policy; static final
								// boolean $assertionsDisabled =
								// !com/bloom/runtime/window/BufWindowFactory.desiredAssertionStatus();
								// }
