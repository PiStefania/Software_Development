#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <pthread.h>
#include "radixHashJoin.h"

#define THREADS 2

typedef struct Job{
	struct Job* nextJob;				//pointer to next job
	void (*function)(void* arg);		//function pointer
	void* arg;							//argument of function pointer
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
	volatile int noAlive;      			//threads currently alive
	volatile int noWorking;    			//threads currently working
	pthread_mutex_t lockThreadPool;     //mutex for locking threadPool
	//pthread_cond_t allNotWorking;    	//cond var for checking if thread pool has no working threads
	pthread_barrier_t barrier;			//helpful if we want to wait all threads' results in order to combine them
}threadPool;

static volatile int keepAlive;			//set to zero, if we want to destroy threads


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

// General Functions for merging data
relation* mergeIntoHist(threadPool* thPool, relation* R);
result* mergeIntoResultList(threadPool* thPool, indexCompareJoinArgs* args);

#endif
