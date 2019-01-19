#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <pthread.h>
#include "radixHashJoin.h"

typedef struct Job{
	struct Job* nextJob;
	void (*function)(void* arg);
	void* arg;
}Job;

typedef struct JobPool{
	Job* head;							//job head -> get node from
	Job* tail;							//job tail -> insert to next of
	int size;							//current size of queue
	pthread_mutex_t lockJobPool;		//mutex for locking jobPool
	pthread_cond_t notEmpty;			//cond var for checking if jobPool is empty
}JobPool;

typedef struct thread{
	pthread_t threadId;					//Thread
	struct threadPool* thPool;			//Need to get access to whole structure for executing a job
}thread;

typedef struct threadPool{
	int noThreads;						//number of threads
	thread* threads; 					//execution threads
	JobPool* jobPool;					//jobs that the threads consume
	int noAlive;      			//threads currently alive
	int noWorking;    			//threads currently working
	pthread_mutex_t lockThreadPool;    //mutex for locking threadPool
	pthread_cond_t allNotWorking;    	//cond var for checking if thread pool has no working threads
}threadPool;

static volatile int keepAlive;


// Functions for jobPool
JobPool* initializeJobPool();
void destroyJobPool(JobPool** jobPool);
void insertJob(JobPool* jobPool, Job* job);
Job* getJob(JobPool* jobPool);

// Functions for threadPool
threadPool* initializeThreadPool(int numThreads);
void destroyThreadPool(threadPool** th);

// General function for executing job
void* executeJob(thread* th);

// General Functions for mergind data
relation* mergeIntoHist(threadPool* thPool, relation* R);
#endif