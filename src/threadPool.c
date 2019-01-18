#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include "../include/threadPool.h"

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
		free(tempJob->arg);
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
		jobPool->head->nextJob = NULL;
	}else{
		jobPool->tail->nextJob = malloc(sizeof(Job));
		jobPool->tail->nextJob->function = job->function;
		jobPool->tail->nextJob->arg = job->arg;
		jobPool->tail = jobPool->tail->nextJob;
		jobPool->tail->nextJob = NULL;
	}
	free(job);
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

// Functions for threadPool
threadPool* initializeThreadPool(int numThreads){
	// If number of threads given is 0, return NULL
	if(numThreads == 0){
		return NULL;
	}

	// Allocate thread pool
	threadPool* thPool = malloc(sizeof(threadPool));
	if(thPool == NULL){
		return NULL;
	}
	// Set each field
	// Set number of threads to thread pool
	thPool->noThreads = numThreads;
	// Threads currently alive are 0
	thPool->noAlive = 0;
	// Threads currently working are also 0
	thPool->noWorking = 0;
	// Initialize job queue
	thPool->jobPool = initializeJobPool();
	if (thPool->jobPool == NULL){
		destroyThreadPool(&thPool);
		return NULL;
	}
	// Allocate threads
	thPool->threads = malloc(numThreads * sizeof(thread));
	if (thPool->threads == NULL){
		destroyThreadPool(&thPool);
		return NULL;
	}
	// Initialize mutex and cond var
	if(pthread_mutex_init(&thPool->lockThreadPool, NULL) != 0){
		destroyThreadPool(&thPool);
		return NULL;
	}
	if(pthread_cond_init(&thPool->allNotWorking, NULL) != 0){
		destroyThreadPool(&thPool);
		return NULL;
	}
	// In addition, we want to keep them alive (until it is set to 0)
	keepAlive = 1;

	// Initialize threads
	for(int i=0; i < numThreads; i++){
		// Set access to thread pool
		thPool->threads[i].thPool = thPool;
		// Initialize each thread
		pthread_create(&thPool->threads[i].threadId, NULL, (void *) executeJob, &thPool->threads[i]);
		// No need to join threads at end
		pthread_detach(thPool->threads[i].threadId);
	}

	return thPool;
}

// Destroy thread pool
void destroyThreadPool(threadPool** thPool){
	if (thPool == NULL){
		return;
	}
	// Threads should end
	keepAlive = 0;

	// Wake all threads
	while ((*thPool)->noAlive){
		pthread_mutex_lock(&(*thPool)->jobPool->lockJobPool);
		pthread_cond_broadcast(&(*thPool)->jobPool->notEmpty);
		pthread_mutex_unlock(&(*thPool)->jobPool->lockJobPool);
		sleep(1);
	}

	// Destroy job pool
	destroyJobPool(&(*thPool)->jobPool);

	// Destroy threads
	if((*thPool)->threads!=NULL){
		free((*thPool)->threads);
		(*thPool)->threads = NULL;
	}

	(*thPool)->noThreads = -1;

    pthread_mutex_destroy(&((*thPool)->lockThreadPool));
    pthread_cond_destroy(&((*thPool)->allNotWorking));

	free((*thPool));
	*thPool = NULL;
}

void* executeJob(thread* th){
	//printf("EXECUTE: %ld\n",th->threadId);
	threadPool* thPool = th->thPool;
	// Set thread as alive
	pthread_mutex_lock(&thPool->lockThreadPool);
	thPool->noAlive += 1;
	pthread_mutex_unlock(&thPool->lockThreadPool);

	while(keepAlive){
		// wait until job pool has jobs
		pthread_mutex_lock(&(thPool->jobPool->lockJobPool));
		if(thPool->jobPool->size <= 0) {
			pthread_cond_wait(&(thPool->jobPool->notEmpty), &(thPool->jobPool->lockJobPool));
		}
		pthread_mutex_unlock(&(thPool->jobPool->lockJobPool));
		// If threads are kept alive
		if (keepAlive && thPool->jobPool->size > 0){
			//printf("WORK: %ld\n",th->threadId);
			pthread_mutex_lock(&thPool->lockThreadPool);
			// Set current thread as working
			thPool->noWorking++;
			pthread_mutex_unlock(&thPool->lockThreadPool);
			// Pop job
			Job* job = getJob(thPool->jobPool);
			// Execute job
			job->function(job->arg);
			//printf("RET\n");
			// Free job object
			free(job->arg);
			free(job);
			pthread_mutex_lock(&thPool->lockThreadPool);
			thPool->noWorking--;
			if (thPool->noWorking == 0) {
				// Signal that all threads are not working
				pthread_cond_signal(&thPool->allNotWorking);
			}
			pthread_mutex_unlock(&thPool->lockThreadPool);
		}
	}

	pthread_mutex_lock(&thPool->lockThreadPool);
	// Decrement number of threads being alive
	thPool->noAlive--;
	pthread_mutex_unlock(&thPool->lockThreadPool);
	return NULL;
}


relation* mergeIntoHist(threadPool* thPool, relation* R){
	// Use threads for creating SHist by cutting it to pieces as the number of threads exist
    relation** rels = malloc(thPool->noThreads*sizeof(relation*));
   	int remainingTuples = R->num_tuples % thPool->noThreads;
    // Perfect division - easy cut
    int divisionFlag = 0;
    if(remainingTuples == 0){
    	divisionFlag = 1;
    }

    // Create Hists array
    relation** hists = malloc(thPool->noThreads*sizeof(relation*));
	int rowsEachChunk = R->num_tuples / thPool->noThreads;
	int tempTuples = 0;
	for(int i=0;i<thPool->noThreads;i++){
		// Allocate and set chunk relations
		// Last chunk will have additional rows if divisionFlag == 0
		if(i==thPool->noThreads-1 && divisionFlag == 0){
			rowsEachChunk += remainingTuples;
		}
		rels[i] = malloc(sizeof(relation));
		rels[i]->tuples = malloc(rowsEachChunk*sizeof(tuple));
		memmove(rels[i]->tuples, R->tuples + tempTuples, rowsEachChunk * sizeof(tuple));
		rels[i]->num_tuples = rowsEachChunk;
		tempTuples += rowsEachChunk;
		// Add chunk and hist creation function
		// Create histArgs struct
		histArgs* args = malloc(sizeof(histArgs));
		args->Hist = &hists[i];
		args->R = rels[i];
		// Add to job Pool
		Job* job = malloc(sizeof(Job));
		job->function = (void*)createHistogramThread;
		job->arg = args;
		//printf("ADD WORK\n");
		insertJob(thPool->jobPool, job);
	}

	/*pthread_mutex_lock(&(thPool->lockThreadPool));
	while(thPool->noWorking <= 0) {
		pthread_cond_wait(&(thPool->allNotWorking), &(thPool->lockThreadPool));
	}
	pthread_mutex_unlock(&(thPool->lockThreadPool));*/

    // Create histogram
    // Merge all chunks of Hists to a single Hist
    // Same number of tuples for each Hist == BUCKETS
    int numTuplesHists = hists[0]->num_tuples;
    relation* Hist = malloc(sizeof(relation));
    Hist->num_tuples = numTuplesHists;
    Hist->tuples = malloc(numTuplesHists*sizeof(tuple));

	// Merge Hists to one by adding each row
	for(int j=0;j<thPool->noThreads;j++){
		for(int i=0;i < numTuplesHists ;i++){
			Hist->tuples[i].rowId = hists[j]->tuples[i].rowId;
			if(i == 0){
				Hist->tuples[i].value = hists[j]->tuples[i].value;
			}else{
				Hist->tuples[i].value += hists[j]->tuples[i].value;
			}
		}
	}

	// Delete additional vars
    for(int i=0;i<thPool->noThreads;i++){
    	deleteRelation(&rels[i]);
    	deleteRelation(&hists[i]);
    }
    free(rels);
    free(hists);

	return Hist;
}
