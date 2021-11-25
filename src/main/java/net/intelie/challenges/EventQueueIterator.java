package net.intelie.challenges;

import java.util.concurrent.PriorityBlockingQueue;

public class EventQueueIterator implements EventIterator {

	private PriorityBlockingQueue<Event> eventQueue;	
	private PriorityBlockingQueue<Event> queryQueue;
	private Event currentEvent;
	private boolean initialized;
	private boolean validQueue;
		
	public EventQueueIterator(PriorityBlockingQueue<Event> eventQueue, PriorityBlockingQueue<Event> queryQueue) {
		super();
		this.eventQueue = eventQueue;
		this.queryQueue = queryQueue;
		this.validQueue = false;
		this.initialized = false;
	}

	@Override
	public void close() throws Exception {
		eventQueue.clear();
		queryQueue.clear();		
	}

	@Override
	public boolean moveNext() {		
		/* Does not remove head of the queue if not initialized */
		if (initialized) {			
			queryQueue.poll();			
		}		
		initialized = true;
		return updateCurrentEvent();		
	}

	@Override
	public Event current() {			
		if(validQueue)
			return currentEvent;
		else
			throw new IllegalStateException("Queue is empty or not initialized.");			
	}

	@Override
	public void remove() {			
		if(validQueue) {
			eventQueue.remove(currentEvent);
			queryQueue.poll();
			updateCurrentEvent();
		}
		else
			throw new IllegalStateException("Queue is empty or not initialized.");
	}
	
	private boolean updateCurrentEvent() {
		currentEvent = queryQueue.peek();
		if (currentEvent == null) { 
			validQueue = false;	
			return false;
		}
		else { 
			validQueue = true;
			return true;
		}
	}

}
