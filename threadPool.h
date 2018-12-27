#ifndef _THREAD_POOL_H_
#define _THREAD_POOL_H_

#include <pthread.h>

typedef struct Job{
	//arguments
}Job;

typedef struct JobPool{
	Job* jobs;				//jobs -> different kind of arguments
	int end;				//end of buffer
	int position;			//current size of buffer
	int start;
}JobPool;

typedef struct threadPool{
	int noThreads;						//number of threads
	pthread_t* tids; 					//execution threads
	pthread_cond_t notEmpty;			//cond var for checking if jobPool is empty
	pthread_cond_t notFull;				//cond var for checking if jobPool is full
	pthread_mutex_t lockJobPool;		//mutex for locking jobPool
	JobPool* jobPool;						//jobs that the threads consume
}threadPool;

//functions for jobPool
JobPool* initializeJobPool();
void insertJob(threadPool* th, Job* job);
Job* getJob(threadPool* th);
void destroyJobPool(JobPool** jobPool);
void* executeJob(void* args);

//functions for threads
threadPool* initializeThreadPool(int numThreads, int kindThread);
void destroyThreadPool(threadPool** th);

#endif