"""Spark Connect session manager with pooling."""
import time
from typing import Dict, Optional
from threading import Lock
from app.spark.connect_client import SparkConnectClient
from app.config import get_settings

settings = get_settings()


class SessionPool:
    """Pool of Spark Connect sessions."""

    def __init__(self, max_size: int = 10, idle_timeout: int = 1800):
        """
        Initialize session pool.

        Args:
            max_size: Maximum number of sessions per cluster
            idle_timeout: Idle timeout in seconds (default: 30 minutes)
        """
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self._pool: Dict[str, list] = {}  # cluster_id -> list of (client, last_used)
        self._lock = Lock()

    def get_client(self, cluster_id: str, connect_url: str, token: str) -> SparkConnectClient:
        """
        Get or create a Spark Connect client from the pool.

        Args:
            cluster_id: Cluster ID
            connect_url: Spark Connect URL
            token: Authentication token

        Returns:
            SparkConnectClient instance
        """
        with self._lock:
            # Initialize pool for this cluster if needed
            if cluster_id not in self._pool:
                self._pool[cluster_id] = []

            # Try to reuse an existing client
            for i, (client, last_used) in enumerate(self._pool[cluster_id]):
                # Check if client is still valid
                if time.time() - last_used < self.idle_timeout:
                    # Remove from pool and return
                    self._pool[cluster_id].pop(i)
                    return client

            # No available client, create new one
            client = SparkConnectClient(connect_url, token)
            client.connect()  # Initialize connection
            return client

    def return_client(self, cluster_id: str, client: SparkConnectClient):
        """
        Return a client to the pool.

        Args:
            cluster_id: Cluster ID
            client: SparkConnectClient to return
        """
        with self._lock:
            if cluster_id not in self._pool:
                self._pool[cluster_id] = []

            # Only add back if pool is not full
            if len(self._pool[cluster_id]) < self.max_size:
                self._pool[cluster_id].append((client, time.time()))
            else:
                # Pool is full, close the session
                client.stop()

    def cleanup_idle_sessions(self):
        """Clean up idle sessions that exceed timeout."""
        with self._lock:
            current_time = time.time()

            for cluster_id in list(self._pool.keys()):
                # Filter out expired sessions
                active_sessions = []
                for client, last_used in self._pool[cluster_id]:
                    if current_time - last_used < self.idle_timeout:
                        active_sessions.append((client, last_used))
                    else:
                        # Session expired, close it
                        client.stop()

                self._pool[cluster_id] = active_sessions

    def close_all(self):
        """Close all sessions in the pool."""
        with self._lock:
            for cluster_id in self._pool:
                for client, _ in self._pool[cluster_id]:
                    client.stop()
            self._pool.clear()


# Global session pool instance
_session_pool: Optional[SessionPool] = None


def get_session_pool() -> SessionPool:
    """Get the global session pool instance."""
    global _session_pool
    if _session_pool is None:
        _session_pool = SessionPool(
            max_size=settings.spark_session_pool_size,
            idle_timeout=settings.spark_session_idle_timeout,
        )
    return _session_pool
