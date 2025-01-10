/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.task

import io.airbyte.cdk.load.command.DestinationConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.mina.util.ConcurrentHashSet

interface WrappedTask : Task {
    val innerTask: Task
}

@Singleton
class TaskScopeProvider(config: DestinationConfiguration) {
    private val log = KotlinLogging.logger {}

    private val timeoutMs = config.gracefulCancellationTimeoutMs

    private val supervisor = Job()
    private val ioScope = CoroutineScope(Dispatchers.IO + supervisor)
    private val defaultScope = CoroutineScope(Dispatchers.Default + supervisor)
    private val killOnSyncFailure = ConcurrentHashSet<Job>()
    private val cancelAtEndOfSync = ConcurrentHashSet<Job>()

    suspend fun launch(task: Task) {
        val scope = if (task.isIO) ioScope else defaultScope
        val job =
            scope.launch {
                log.info { "Launching $task" }
                task.execute()
                log.info { "Task $task completed" }
            }
        if (task.cancelAtEndOfSync) {
            cancelAtEndOfSync.add(job)
        }
        if (task.killOnSyncFailure) {
            killOnSyncFailure.add(job)
        }
    }

    suspend fun close() {
        log.info { "Closing normally, canceling long-running tasks" }
        cancelAtEndOfSync.forEach { it.cancel() }

        val uncaughtExceptions = AtomicReference<Throwable>()
        log.info { "Verifying task completion" }
        supervisor.children.forEach {
            it.invokeOnCompletion { cause ->
                if (cause != null) {
                    log.error { "Uncaught exception in task: $cause" }
                    uncaughtExceptions.set(cause)
                }
            }
        }
        if (uncaughtExceptions.get() != null) {
            throw uncaughtExceptions.get()
        }
    }

    suspend fun kill() {
        log.info { "Failing, killing input tasks and canceling long-running tasks" }
        killOnSyncFailure.forEach { it.cancel() }
        cancelAtEndOfSync.forEach { it.cancel() }

        // Give the implementor tasks a chance to fail gracefully
        withTimeoutOrNull(timeoutMs) {
            log.info {
                "Cancelled internal tasks, waiting ${timeoutMs}ms for implementor tasks to complete"
            }
            supervisor.complete()
            log.info { "Implementor tasks completed" }
        }
            ?: run {
                log.error { "Implementor tasks did not complete within ${timeoutMs}ms, cancelling" }
                supervisor.cancel()
            }
    }
}
