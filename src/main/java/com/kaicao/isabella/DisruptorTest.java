package com.kaicao.isabella;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DisruptorTest {

    public static final AtomicLong executes = new AtomicLong();

    public static class TestEvent {
        private long event;

        public long getEvent() {
            return event;
        }

        public void setEvent(long event) {
            this.event = event;
        }
    }

    public static class TestEventFactory implements EventFactory<TestEvent> {
        public TestEvent newInstance() {
            return new TestEvent();
        }
    }

    public static class TestEventHandler implements EventHandler<TestEvent> {
        public void onEvent(TestEvent testEvent, long sequence, boolean endOfBatch) throws Exception {
            //System.out.println(testEvent.getEvent());
            executes.incrementAndGet();
        }
    }

    public static class TestEventWorkHandler implements WorkHandler<TestEvent> {
        @Override
        public void onEvent(TestEvent event) throws Exception {
            executes.incrementAndGet();
        }
    }

    public static class TestEventProducer {
        private static final EventTranslatorOneArg<TestEvent, Long> TRANSLATOR = new EventTranslatorOneArg<TestEvent, Long>() {
            public void translateTo(TestEvent event, long sequence, Long arg0) {
                event.setEvent(arg0);
            }
        };
        private final RingBuffer<TestEvent> ringBuffer;

        public TestEventProducer(RingBuffer<TestEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void onData(long value) {
            /* Before 3.0
            long sequence = ringBuffer.next(); // Get next sequence, Calls of this method should ensure that they always publish the sequence afterward.
            try {
                TestEvent event = ringBuffer.get(sequence); // Get entry in Disruptor by sequence
                event.setEvent(value);
            } finally {
                ringBuffer.publish(sequence);   // Ensure always published due to ringBuffer.next() requirement. Failing to do can result in corruption of the state of the Disruptor.
            }
            */

            // After 3.0
            ringBuffer.publishEvent(TRANSLATOR, value);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Start Main");
        // Disruptor doc: https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started
        // Example: http://java-is-the-new-c.blogspot.com.au/2014/01/comparision-of-different-concurrency.html
        // 1. Create event POJO, wrapper of data
        // 2. Create EventFactory implementation
        // 3. Create consumer (EventHandler)

        // Consumer executor
        Executor consumers = Executors.newFixedThreadPool(2);
        TestEventFactory factory = new TestEventFactory();
        // Specify size of ringbuffer, must be power of 2
        int bufferSize = 16_384;
        Disruptor<TestEvent> disruptor = new Disruptor<>(factory, bufferSize, consumers, ProducerType.SINGLE, new SleepingWaitStrategy());
        // Connect with handler
        disruptor.handleEventsWith(new TestEventHandler());
        //disruptor.handleEventsWithWorkerPool(createWorkPool(4));
        System.out.println("Start Disruptor");
        // Start disruptor, starts all threads running
        // Get ringbuffer from disruptor to be used for publishing
        RingBuffer<TestEvent> ringBuffer = disruptor.start();
        TestEventProducer producer = new TestEventProducer(ringBuffer);

        long counter = 0l;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() <= startTime + TimeUnit.SECONDS.toMillis(60)) {
            producer.onData(counter++);
        }
        System.out.println("Wait to finish");
        disruptor.shutdown();
        System.out.println("Finished with " + executes.get() + " execution");
        System.exit(1);
    }

    private static TestEventWorkHandler[] createWorkPool(int amount) {
        TestEventWorkHandler[] result = new TestEventWorkHandler[amount];
        for (int i = 0; i < amount; i ++) {
            result[i] = new TestEventWorkHandler();
        }
        return result;
    }

    /**
     *
     OS X version 10.9.5, 2.7GHz Intel Core i5, 8G 1600MHz DDR3
     Java 1.8.0_20.jdk

     Only 1 core fully loaded when use handleEventsWith(EventHandler), all cores fully loaded when use handleEventsWithWorkerPool(WorkPool)
     and handleEventsWith is more efficient both CPU usage and execution cycles

     Last	RB size	C. pool	P. Strat.	W.Strat.		C. execution

     60s	16_384	2 fixed	SINGLE	SLEEPWAIT	1,169,176,405
     60s	16_384	8 fixed	SINGLE	SLEEPWAIT	1,180,177,835
     60s	16_384	16fixed	SINGLE	SLEEPWAIT	1,198,897,808
     60s	16_384	cached	SINGLE 	SLEEPWAIT	1,173,763,690
     // MULTI provider mode slower the performance
     60s	16_384	4 fixed	MULTI	SLEEPWAIT	   747,067,164
     60s	16_384	8 fixed	MULTI	SLEEPWAIT	   847,220,435
     60s	16_384	16 fixed	MULTI	SLEEPWAIT	   844,376,891
     60s	16_384	cached	MULTI 	SLEEPWAIT	   870,458,627

     // Much more CPU System time compare to SLEEPWAIT, around 20%
     60s	16_384	8 fixed	SINGLE	BLOCKWAIT	   328,355,550
     60s	16_384	cached	SINGLE 	BLOCKWAIT	   325,253,292

     // Lower CPU System time under 1.5%
     60s	16_384	8 fixed	SINGLE	YIELDWAIT	   1,155,746,852
     60s	16_384	cached	SINGLE 	YIELDWAIT	   1,193,577,829

     // Lower CPU System time under 1.5%
     60s	16_384	2 fixed	SINGLE	BUSYSPIN	   1,196,465,024
     60s	16_384	8 fixed	SINGLE	BUSYSPIN	   1,197,655,730
     60s	16_384	cached	SINGLE 	BUSYSPIN	   1,157,914,064

     // Use WorkPool (all cores fully loaded)
     4	60s	16_384	cached	SINGLE 	SLEEPWAIT	540,653,472
     8	60s	16_384	cached	SINGLE 	SLEEPWAIT	634,761,534
     16	60s	16_384	cached	SINGLE 	SLEEPWAIT	644,594,424
     */
}
