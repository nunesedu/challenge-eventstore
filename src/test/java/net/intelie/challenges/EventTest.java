package net.intelie.challenges;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Vector;

public class EventTest {
	
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    } 
    
    @Test
    public void thisIsADisclaimer() throws Exception {
    	
		//DISCLAIMER:
		//Some are called freaks for different reasons.
    	assertEquals(Float.POSITIVE_INFINITY, Float.intBitsToFloat(0b01111111100000000000000000000000), 0);
    } 
    
    private static EventQueue eventQueue;
    private static Vector<Event> events;
    private static int N = 14;
    private final static int N_EXECS = 1000;
    private EventIterator eventQueryIterator;    
    
    /**
     * Initialize structures
     *
     */    
    public static void setUpEvents() {
    	events = new Vector<Event>(N);
		for(int i =0; i<N; i++) events.add(null);
		events.set(13,  new Event("d", 9223372036854775807L)); // max long
		events.set(9, new Event("c", 109L));
		events.set(3, new Event("b", 003L));
		events.set(1, new Event("", 001L));
		events.set(7, new Event("c", 107L));
		events.set(6, new Event("c", 006L));
		events.set(10, new Event("d", 0L));
		events.set(8, new Event("c", 108L));
		events.set(11, new Event("d", 100000570L));
		events.set(4, new Event("b", 004L));
		events.set(2, new Event("", 002L));
		events.set(5, new Event("b", 005L));
		events.set(0, new Event("", 000L));
		events.set(12, new Event("d", 111122999999L));
    }
    
    public static void setUpQueue() {
    	eventQueue = new EventQueue();
    	eventQueue.insert(events.get(13));
    	eventQueue.insert(events.get(9));
    	eventQueue.insert(events.get(3));
    	eventQueue.insert(events.get(1));
    	eventQueue.insert(events.get(7));
    	eventQueue.insert(events.get(6));
    	eventQueue.insert(events.get(10));
    	eventQueue.insert(events.get(8));
    	eventQueue.insert(events.get(11));
    	eventQueue.insert(events.get(4));
    	eventQueue.insert(events.get(2));
    	eventQueue.insert(events.get(5));
    	eventQueue.insert(events.get(0));
    	eventQueue.insert(events.get(12));
    }
    
    public static void setUp() {
    	setUpEvents();
    	setUpQueue();    	
    }

    /**
     * Tests eventQueue ordering
     *
     */ 
    @Test    
    public void test0() throws Exception {

    	setUp();
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(0));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(1));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(2));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(3));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(4));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(5));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(6));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(7));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(8));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(9));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(10));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(11));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(12));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(13));

    }
    
    /**
     * Tests removeAll type "" (empty string)
     *
     */ 
    @Test
    public void test1() throws Exception {
    	setUp();
    	eventQueue.removeAll("");
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(3));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(4));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(5));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(6));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(7));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(8));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(9));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(10));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(11));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(12));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(13));
    }    
    
    /**
     * Tests removeAll type "b"
     *
     */ 
    @Test
    public void test2() throws Exception {
    	setUp();
    	eventQueue.removeAll("b");
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(0));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(1));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(2));    	
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(6));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(7));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(8));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(9));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(10));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(11));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(12));
    	assertEquals(eventQueue.getEventQueue().poll(), events.get(13));
    }
    
    
    /**
     * Tests inclusive startTime and exclusive endTime
     *
     */ 
    @Test
    public void test5() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("c", 006L, 107L);
    	eventQueryIterator.moveNext();
    	assertEquals(eventQueryIterator.current(), events.get(6));
    	eventQueryIterator.moveNext();
    	assertThrows(IllegalStateException.class, () -> eventQueryIterator.current());
    	
    	eventQueryIterator = eventQueue.query("b", 004L, 005L);
    	eventQueryIterator.moveNext();
    	assertEquals(eventQueryIterator.current(), events.get(4));
    	eventQueryIterator.moveNext();
    	assertThrows(IllegalStateException.class, () -> eventQueryIterator.current());
    	
    	eventQueryIterator = eventQueue.query("b", 004L, 006L);
    	eventQueryIterator.moveNext();
    	assertEquals(eventQueryIterator.current(), events.get(4));
    	eventQueryIterator.moveNext();
    	assertEquals(eventQueryIterator.current(), events.get(5));

    }
    
    /**
     * Tests remove
     *
     */
    @Test
    public void test6() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("", 000L, 005L);
    	eventQueryIterator.moveNext();
    	assertEquals(eventQueue.getEventQueue().contains(events.get(0)), true);
    	assertEquals(eventQueryIterator.current(), events.get(0));    	
    	eventQueryIterator.remove();
    	assertEquals(eventQueue.getEventQueue().contains(events.get(0)), false);
    	assertEquals(eventQueryIterator.current(), events.get(1));
    	assertEquals(eventQueue.getEventQueue().contains(events.get(1)), true);
    	eventQueryIterator.remove();
    	assertEquals(eventQueue.getEventQueue().contains(events.get(1)), false);
    	assertEquals(eventQueryIterator.current(), events.get(2));
    	assertEquals(eventQueue.getEventQueue().contains(events.get(2)), true);
    	eventQueryIterator.remove();
    	assertEquals(eventQueue.getEventQueue().contains(events.get(2)), false);
    	assertThrows(IllegalStateException.class, () -> eventQueryIterator.current());    	
    }
    
    /**
     * Tests remove() when moveNext was never called
     *
     */
    @Test
    public void test7() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("b", 000L, 100L);
    	assertThrows(IllegalStateException.class, () -> eventQueryIterator.remove());
    }
        
    /**
     * Tests current() when moveNext was never called
     *
     */
    @Test
    public void test8() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("b", 000L, 100L);
    	assertThrows(IllegalStateException.class, () -> eventQueryIterator.current());
    }
    
    /**
     * Tests moveNext() on empty queue
     *
     */
    @Test
    public void test9() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("b", 999999L, 99999999L);
    	assertEquals(eventQueryIterator.moveNext(), false);    
    }
    
    /**
     * Tests current() and moveNext() until the end of the queue
     *
     */
    @Test
    public void test10() throws Exception {
    	setUp();
    	eventQueryIterator = eventQueue.query("c", 000L, 1000L);
    	assertEquals(eventQueryIterator.moveNext(), true);
    	assertEquals(eventQueryIterator.current(), events.get(6));   
    	assertEquals(eventQueryIterator.moveNext(), true);
    	assertEquals(eventQueryIterator.current(), events.get(7)); 
    	assertEquals(eventQueryIterator.moveNext(), true);
    	assertEquals(eventQueryIterator.current(), events.get(8));
    	assertEquals(eventQueryIterator.moveNext(), true);
    	assertEquals(eventQueryIterator.current(), events.get(9));
    	assertEquals(eventQueryIterator.moveNext(), false);    	
    }
           
    
    /**
     *  This test executes N_EXECS times the insertion from N threads. It does not proof but provides an evidence 
     *  of PriorityBlockingQueue's thread-safety, since the queue contains all events and they are properly ordered
     *
     */  
	@Test
	public void test11() throws InterruptedException {
		
		for(int i=0; i<N_EXECS; i++) {
				
			setUpEvents();
			eventQueue = new EventQueue();
					
			for (int j = 0; j < N; j++) {
				new Thread(new queueTester(), String.valueOf(j)).start();
			}
			
			/* Enough time for all threads to finish */
			Thread.sleep(10);
			
			assertEquals(eventQueue.getEventQueue().poll(), events.get(0));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(1));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(2));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(3));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(4));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(5));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(6));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(7));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(8));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(9));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(10));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(11));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(12));
	    	assertEquals(eventQueue.getEventQueue().poll(), events.get(13));
		}
		
	}
    
    static class queueTester implements Runnable {
		
		@Override
		public void run() {				
			int threadId = Integer.parseInt(Thread.currentThread().getName());
			Event event = EventTest.events.get(threadId);			
			EventTest.eventQueue.insert(event);
		}
		
	}
    
}

