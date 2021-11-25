package net.intelie.challenges;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class EventQueue implements EventStore{
	
	/*
	 *  PriorityBlockingQueue
	 *  - Structure implemented as a binary heap, provides O(log(N)) performance on addition and removal
	 *  - Thread-safe
	 *  - A Comparator can be used for custom ordering of objects in the queue  
	 *  
	 * */
	private PriorityBlockingQueue<Event> eventQueue;	
		
	private final static int N = 14;
		
	public EventQueue() {
		eventQueue = new PriorityBlockingQueue<Event>(N, new EventComparator());
	}

	@Override
	public void insert(Event event) {
		eventQueue.put(event);		
	}

	@Override
	public void removeAll(String type) {
		eventQueue.removeIf(event -> event.type().equals(type));		
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		
		PriorityBlockingQueue<Event> queryQueue = new PriorityBlockingQueue<Event>(100, new EventComparator());
		EventQueueIterator eventQueueIterator = new EventQueueIterator(eventQueue, queryQueue);
	
		Iterator<Event> queueIterator = eventQueue.iterator();
		while (queueIterator.hasNext()) {
			Event event = queueIterator.next();
			if (event.type().equals(type)) 
				/* Inclusive start time and exclusive end time */
				if(event.timestamp() >= startTime && event.timestamp() < endTime) 
					queryQueue.add(event);
		}
		
		return eventQueueIterator;
	}
	
	/* For testing purposes  */
	protected PriorityBlockingQueue<Event> getEventQueue() {		
		return eventQueue;
	}
}

class EventComparator implements Comparator<Event> {    
	@Override
    public int compare(Event e1, Event e2) {
		/* Sort first by type */
		if(e1.type().compareTo(e2.type()) < 0)
			return -1;
		else if (e1.type().compareTo(e2.type()) > 0)
			return 1;
		else {
			/* Then sort by timestamp */
	        if (e1.timestamp() < e2.timestamp())
	            return -1;
	        else if (e1.timestamp() > e2.timestamp())
	            return 1;
	        else
	        	return 0;
	    }
	}
}
