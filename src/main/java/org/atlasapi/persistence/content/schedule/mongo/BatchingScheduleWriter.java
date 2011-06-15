package org.atlasapi.persistence.content.schedule.mongo;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

import com.google.common.collect.Lists;

public class BatchingScheduleWriter implements ScheduleWriter {
	
	private final BlockingQueue<Item> itemQueue = new ArrayBlockingQueue<Item>(100000);
	
	private final int BATCH_SIZE = 10;
	
	private AtomicBoolean running = new AtomicBoolean(true);
	
	public BatchingScheduleWriter(final ScheduleWriter writer, final AdapterLog log) {
		Executors.newFixedThreadPool(1).submit(new Runnable() {

			@Override
			public void run() {
				List<Item> toProcess = Lists.newArrayListWithCapacity(BATCH_SIZE);
				
				while (running.get()) {
					if (toProcess.size() == BATCH_SIZE) {
						try {
							writer.writeScheduleFor(toProcess);
						} catch (Exception e) {
							log.record(new AdapterLogEntry(Severity.ERROR).withSource(getClass()).withDescription("Schedule writing fail: schedule will be inconsistent").withCause(e));
						} finally {
							toProcess.clear();
						}
					} else {
						try {
							toProcess.add(itemQueue.take());
						} catch (InterruptedException e) {
							return;
						}
					}
				}
			}
		});
	}

	@Override
	public void writeScheduleFor(Iterable<? extends Item> items) {
//		for (Item item : items) {
//			if (!itemQueue.offer(item)) {
//				log.record(new AdapterLogEntry(Severity.ERROR).withSource(getClass()).withDescription("Schedule writing queue full: schedule will be stale"));
//			}
//		}
	}
}
