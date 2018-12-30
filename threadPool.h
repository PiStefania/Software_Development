#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <pthread.h>

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

typedef struct threadPool{
	int noThreads;						//number of threads
	pthread_t* tids; 					//execution threads
	JobPool* jobPool;					//jobs that the threads consume
	volatile int noAlive;      			//threads currently alive
	volatile int noWorking;    			//threads currently working
	pthread_mutex_t  lockThreadPool;    //mutex for locking threadPool
	pthread_cond_t  allIdle;    		//cond var for checking if thread pool is full (all working)
}threadPool;

// Functions for jobPool
JobPool* initializeJobPool();
void destroyJobPool(JobPool** jobPool);
void insertJob(JobPool* jobPool, Job* job);
Job* getJob(JobPool* jobPool);

// Functions for threads


// Functions for threadPool
threadPool* initializeThreadPool(int numThreads);
void destroyThreadPool(threadPool** th);

#endif