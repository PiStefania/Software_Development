#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h> 
#include <errno.h> 
#include <string.h>
#include "threadPool.h"

// Functions for jobPool
// Initialize jobPool
JobPool* initializeJobPool(){
	JobPool* jobPool = malloc(sizeof(JobPool));
	jobPool->head = NULL;
	jobPool->tail = NULL;
	jobPool->size = 0;
	if(pthread_mutex_init(&(jobPool->lockJobPool),NULL)!=0){
		//destroy jobPool if error occurs
		destroyJobPool(&jobPool);
		return NULL;
	}
	if(pthread_cond_init(&(jobPool->notEmpty),NULL)!=0){
		// Destroy jobPool if error occurs
		destroyJobPool(&jobPool);
		return NULL;
	}
	return jobPool;
}

// Destroy jobPool
void destroyJobPool(JobPool** jobPool){
	Job* currentJob = (*jobPool)->head;
	Job* tempJob = NULL;
	while(currentJob != NULL){
		tempJob = currentJob;
		currentJob = currentJob->nextJob;
		free(tempJob);
	}
	(*jobPool)->head = NULL;
	(*jobPool)->size = 0;
	pthread_mutex_destroy(&((*jobPool)->lockJobPool));
    pthread_cond_destroy(&((*jobPool)->notEmpty));
	free(*jobPool);
	*jobPool = NULL;
}


//insert to the start of jobs
void insertJob(JobPool* jobPool, Job* job){
	// Lock jobPool because it's going to change
	pthread_mutex_lock(&(jobPool->lockJobPool));
	if(jobPool->head == NULL && jobPool->tail == NULL){
		jobPool->head = malloc(sizeof(Job));
		jobPool->head->function = job->function;
		jobPool->head->arg = job->arg;
		jobPool->tail = jobPool->head;
	}else{
		jobPool->tail->nextJob = malloc(sizeof(Job));
		jobPool->tail->nextJob->function = job->function;
		jobPool->tail->nextJob->arg = job->arg;
		jobPool->tail = jobPool->tail->nextJob;
	}
	jobPool->size++;
	pthread_cond_signal(&jobPool->notEmpty);
	pthread_mutex_unlock(&(jobPool->lockJobPool));
}

// Get first item from jobPool
Job* getJob(JobPool* jobPool){
	Job* job = NULL;
	pthread_mutex_lock(&(jobPool->lockJobPool));
	while(jobPool->size <= 0) {
		pthread_cond_wait(&(jobPool->notEmpty), &(jobPool->lockJobPool));
	}
	// Pop first job (head)
	job = jobPool->head;
	jobPool->head = jobPool->head->nextJob;
	if(jobPool->head == NULL){
		jobPool->tail = NULL;
	}
	jobPool->size--;
	pthread_mutex_unlock(&(jobPool->lockJobPool));
	return job;
}

// Functions for threads


/*
// Functions for threadPool
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
		if(pthread_create(&th->tids[i],NULL, (void*) void, th)!=0){
			//destroy threads
			destroyThreadPool(&th);
			return NULL;
		}
	}
	return th;
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
}*/