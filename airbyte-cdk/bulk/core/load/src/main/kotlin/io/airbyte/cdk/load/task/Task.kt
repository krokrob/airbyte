/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.task

interface Task {
    /**
     * If the task performs any blocking io, even writing to local disk, it should set [isIO] =
     * true. [cancelAtEndOfSync] is for long-running tasks that will otherwise not close.
     * [killOnSyncFailure] is for tasks that close normally under success conditions but should be
     * halted immediately on failure to permit shutdown (like input consuming).
     *
     * TODO: simplify this further.
     */
    val isIO: Boolean
    val cancelAtEndOfSync: Boolean
    val killOnSyncFailure: Boolean

    suspend fun execute()
}

/**
 * A TaskLauncher is responsible for starting and stopping the task workflow, and for managing
 * transitions between tasks.
 */
interface TaskLauncher {
    /**
     * Execute the task workflow. Should dispatch tasks asynchronously and suspend until the
     * workflow is complete.
     */
    suspend fun run()
}
