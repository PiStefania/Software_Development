#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h> 
#include <errno.h> 
#include "threadPool.h"
#define JOB_POOL_SIZE 20

//functions for jobPool
JobPool* initializeJobPool(){
	JobPool* jobPool = malloc(sizeof(JobPool));
	jobPool->jobs = malloc(JOB_POOL_SIZE*sizeof(Job));
	jobPool->end = -1;
	jobPool->position = 0;
	jobPool->start = 0;
	return jobPool;
}

//insert to the end a fd
void insertJob(threadPool* th, Job* job){
	pthread_mutex_lock(&(th->lockJobPool));
	while (th->jobPool->position >= JOB_POOL_SIZE) {
		pthread_cond_wait(&(th->notFull), &(th->lockJobPool));
	}
	//insert to buffer
	th->jobPool->end = (th->jobPool->end + 1) % JOB_POOL_SIZE;
	th->jobPool->jobs[th->jobPool->end] = *job;
	th->jobPool->position++;
	pthread_mutex_unlock(&(th->lockJobPool));
}

//get first item from fds
Job* getJob(threadPool* th){
	Job* job = NULL;
	pthread_mutex_lock(&(th->lockJobPool));
	while(th->jobPool->position <= 0) {
		pthread_cond_wait(&(th->notEmpty), &(th->lockJobPool));
	}
	//get first elem 
	job = &th->jobPool->jobs[th->jobPool->start];
	th->jobPool->start = (th->jobPool->start + 1) % JOB_POOL_SIZE;
	th->jobPool->position--;
	pthread_mutex_unlock(&(th->lockJobPool));
	return job;
}

//destroy jobPoll
void destroyJobPool(JobPool** jobPool){
	free((*jobPool)->jobs);
	(*jobPool)->jobs = NULL;
	(*jobPool)->end = -1;
	(*jobPool)->position = 0;
	free(*jobPool);
	*jobPool = NULL;
}

//functions for threadPool
threadPool* initializeThreadPool(int numThreads, int kindThread){
	threadPool* th = malloc(sizeof(threadPool));
	th->tids = malloc(numThreads*sizeof(pthread_t));
	th->noThreads = numThreads;
	
	if(pthread_mutex_init(&(th->lockJobPool),NULL)!=0){
		//destroy threadPool
		destroyThreadPool(&th);
		return NULL;
	}
	
	if(pthread_cond_init(&(th->notEmpty),NULL)!=0){
		//destroy threadPool
		destroyThreadPool(&th);
		return NULL;
	}
	
	if(pthread_cond_init(&(th->notFull),NULL)!=0){
		//destroy threads
		destroyThreadPool(&th);
		return NULL;
	}
	
	//initialize jobPool
	th->jobPool = initializeJobPool();
	
	for(int i=0;i<numThreads;i++){
		if(pthread_create(&th->tids[i],NULL, (void*) executeJob, th)!=0){
			//destroy threads
			destroyThreadPool(&th);
			return NULL;
		}
	}
	return th;
}

void* executeJob(void* args){
	/*threadPool* th = (threadPool*) args;
	while(1){
		//get fd
		int newsock = getPoolData(th);
		pthread_cond_signal(&th->notFull);
		readGetLinesFromCrawler(newsock,th);
		close(newsock);
	}
	pthread_exit(NULL);*/
	return NULL;
}


//destroy threadPool
void destroyThreadPool(threadPool** th){
	for(size_t i = 0; i<(*th)->noThreads; i++) {
        pthread_cancel((*th)->tids[i]);
        pthread_join((*th)->tids[i], NULL);
    }
			
	if((*th)->tids!=NULL){
		free((*th)->tids);
		(*th)->tids = NULL;
	}
	
	(*th)->noThreads = -1;
	
    pthread_mutex_destroy(&((*th)->lockJobPool));
    pthread_cond_destroy(&((*th)->notEmpty));
    pthread_cond_destroy(&((*th)->notFull));
	
	destroyJobPool(&((*th)->jobPool));
	
	free((*th));
	*th = NULL;
}