/*******************************************************************************
 * Copyright 2016 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

/*
 * Functions that control thread behaviour
 */
#define _XOPEN_SOURCE_EXTENDED 1
#include "pthread.h"
#include "time.h"
#include <sys/time.h>
#include <errno.h>
#include <mach/clock.h>
#include <mach/mach.h>
#include <mach/task.h>
#include <mach/semaphore.h>
#include <sys/sem.h>
#include <sys/ipc.h>
#include <dispatch/dispatch.h>


#include "ibmras/common/port/ThreadData.h"
#include "ibmras/common/port/Semaphore.h"
#include "ibmras/common/logging.h"
#include <map>
#include <stack>
#include <list>

namespace ibmras {
namespace common {
namespace port {

IBMRAS_DEFINE_LOGGER("Port");

std::list<pthread_cond_t> condMap;
std::stack<pthread_t> threadMap;
pthread_mutex_t condMapMux = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t threadMapMux = PTHREAD_MUTEX_INITIALIZER;
bool stopping = false;

void* wrapper(void *params) {
	IBMRAS_DEBUG(fine,"in thread.cpp->wrapper");
	ThreadData* data = reinterpret_cast<ThreadData*>(params);
	void* result;
	if (data->hasStopMethod()) {
		IBMRAS_DEBUG(debug,"stopMethod present");
		pthread_cleanup_push(reinterpret_cast<void (*)(void*)>(data->getStopMethod()), data);
		IBMRAS_DEBUG(debug,"executing callback");
		result = data->getCallback()(data);
		pthread_cleanup_pop(1);
	} else {
		IBMRAS_DEBUG(debug,"stopMethod not present, executing callback");
		result = data->getCallback()(data);
	}
	return result;
}

uintptr_t createThread(ThreadData* data) {
	IBMRAS_DEBUG(fine,"in thread.cpp->createThread");
	uintptr_t retval;
	// lock the threadMap as we might be making updates to it
	pthread_mutex_lock(&threadMapMux);
	if (!stopping) {
		pthread_t thread;
		retval = pthread_create(&thread, NULL, wrapper, data);
		if (retval == 0) {
			IBMRAS_DEBUG(debug,"Thread created successfully");
			// only store valid threads
			threadMap.push(thread);
		}
	} else {
		IBMRAS_DEBUG(debug,"Trying to stop - thread not created");
		retval = ECANCELED;
	}
	pthread_mutex_unlock(&threadMapMux);
	return retval;
}

void exitThread(void *val) {
	IBMRAS_DEBUG(fine,"in thread.cpp->exitThread");
	pthread_exit(NULL);
}

void sleep(uint32 seconds) {
	IBMRAS_DEBUG(fine,"in thread.cpp->sleep");
	/* each sleep has its own mutex and condvar - the condvar will either
		be triggered by condBroadcast or it will timeout.*/
	pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t c = PTHREAD_COND_INITIALIZER;

	IBMRAS_DEBUG(debug,"Updating condvar map");
	// lock the condvar map for update
	pthread_mutex_lock(&condMapMux);
	std::list<pthread_cond_t>::iterator it = condMap.insert(condMap.end(),c);
	pthread_mutex_unlock(&condMapMux);
	pthread_mutex_lock(&m);

	struct timespec t;
	clock_serv_t cclock;
	mach_timespec_t mts;
	host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
	clock_get_time(cclock, &mts);
	mach_port_deallocate(mach_task_self(), cclock);
	t.tv_sec = mts.tv_sec+seconds;
	IBMRAS_DEBUG_1(finest,"Sleeping for %d seconds", seconds);
	pthread_cond_timedwait(&c, &m, &t);
	IBMRAS_DEBUG(finest,"Woke up");
	pthread_mutex_unlock(&m);
	pthread_mutex_lock(&condMapMux);
	condMap.erase(it);
	pthread_mutex_unlock(&condMapMux);
}

void condBroadcast() {
	IBMRAS_DEBUG(fine,"in thread.cpp->condBroadcast");
	//prevent other threads adding to the condMap
	pthread_mutex_lock(&condMapMux);
	for (std::list<pthread_cond_t>::iterator it=condMap.begin(); it!=condMap.end(); ++it) {
		pthread_cond_broadcast(&(*it));
	}
	pthread_mutex_unlock(&condMapMux);
}

void stopAllThreads() {
	IBMRAS_DEBUG(fine,"in thread.cpp->stopAllThreads");
	//prevent new thread creation
	pthread_mutex_lock(&threadMapMux);
	stopping = true;
	// wake currently sleeping threads
	condBroadcast();
	while (!threadMap.empty()) {
		pthread_cancel(threadMap.top());
		//wait for the thread to stop
		pthread_join(threadMap.top(), NULL);
		threadMap.pop();
	}
	pthread_mutex_unlock(&threadMapMux);
}

Semaphore::Semaphore(uint32 initial, uint32 max) {
	if (!stopping) {
		IBMRAS_DEBUG(fine,"in thread.cpp creating CreateSemaphoreA");
        mach_port_t task = mach_task_self();
        kern_return_t err;

		err = semaphore_create(task, reinterpret_cast<semaphore_t*>(handle), SYNC_POLICY_FIFO, initial);
		if(err != KERN_SUCCESS) {
			IBMRAS_DEBUG(warning, "Failed to create semaphore");
		}
	} else {
		IBMRAS_DEBUG(debug,"Trying to stop - semaphore not created");
		handle = NULL;
	}
}

void Semaphore::inc() {
	IBMRAS_DEBUG(finest, "Incrementing semaphore ticket count");
	if(handle) {
        semaphore_t* semTP = reinterpret_cast<semaphore_t*>(handle);
		semaphore_signal(*semTP);
	}
}

bool Semaphore::wait(uint32 timeout) {
	kern_return_t err;
	while(!handle) {
		sleep(timeout);		/* wait for the semaphore to be established */
	}
	IBMRAS_DEBUG(finest, "semaphore wait");
    mach_timespec_t ts;
    ts.tv_sec = timeout;
    ts.tv_nsec = 0;
    semaphore_t* semTP = reinterpret_cast<semaphore_t*>(handle);
	err = semaphore_timedwait(*semTP, ts);
	if(err == KERN_SUCCESS) {
		IBMRAS_DEBUG(finest, "semaphore posted");
		return true;
	}

	IBMRAS_DEBUG(finest, "semaphore timeout");
	return (err != KERN_OPERATION_TIMED_OUT);
}

Semaphore::~Semaphore() {
    mach_port_t task = mach_task_self();
    semaphore_t* semTP = reinterpret_cast<semaphore_t*>(handle);
	semaphore_destroy(task, *semTP);
}

}
}
}		/* end namespace port */
