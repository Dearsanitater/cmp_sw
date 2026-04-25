import threading


class DbSession:
    def __init__(self, connection, db_key, db_type):
        self.connection = connection
        self.db_key = db_key
        self.db_type = db_type
        self._last_cursor = None

    @property
    def raw_connection(self):
        return self.connection

    def cursor(self):
        self._last_cursor = self.connection.cursor()
        return self._last_cursor

    def _active_cursor(self):
        return self._last_cursor or self.cursor()

    def execute(self, sql, params=None):
        cursor = self._active_cursor()
        if params is None:
            return cursor.execute(sql)
        return cursor.execute(sql, params)

    def executemany(self, sql, params):
        cursor = self._active_cursor()
        return cursor.executemany(sql, params)

    def fetchall(self):
        return self._active_cursor().fetchall()

    def fetchone(self):
        return self._active_cursor().fetchone()

    def commit(self):
        method = getattr(self.connection, "commit", None)
        if callable(method):
            return method()
        return None

    def rollback(self):
        method = getattr(self.connection, "rollback", None)
        if callable(method):
            return method()
        return None

    def close(self):
        method = getattr(self.connection, "close", None)
        if callable(method):
            return method()
        return None

    def autocommit(self, enabled):
        handler = getattr(self.connection, "autocommit", None)
        if callable(handler):
            return handler(enabled)
        if handler is not None:
            setattr(self.connection, "autocommit", enabled)
        return None

    def __getattr__(self, item):
        return getattr(self.connection, item)


class Db2Session(DbSession):
    def __init__(self, connection, db_key, db_type="db2"):
        super().__init__(connection=connection, db_key=db_key, db_type=db_type)
        self.stmt = None
        self._rows = []
        self._fetch_index = 0

    def _ibm_db(self):
        import ibm_db

        return ibm_db

    def cursor(self):
        self._last_cursor = self
        return self

    def execute(self, sql, params=None):
        ibm_db = self._ibm_db()
        self.stmt = ibm_db.prepare(self.connection, sql)
        if params is None:
            result = ibm_db.execute(self.stmt)
        else:
            result = ibm_db.execute(self.stmt, params)
        self._rows = []
        self._fetch_index = 0
        return result

    def fetchall(self):
        ibm_db = self._ibm_db()
        rows = []
        row = ibm_db.fetch_tuple(self.stmt)
        while row:
            rows.append(row)
            row = ibm_db.fetch_tuple(self.stmt)
        self._rows = rows
        self._fetch_index = len(rows)
        return rows

    def fetchone(self):
        if self._rows and self._fetch_index < len(self._rows):
            row = self._rows[self._fetch_index]
            self._fetch_index += 1
            return row
        rows = self.fetchall()
        return rows[0] if rows else None

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        ibm_db = self._ibm_db()
        try:
            return ibm_db.close(self.connection)
        except Exception:
            return None


class ThreadLocalSessionProxy:
    is_thread_local_proxy = True

    def __init__(self, session_factory):
        self._session_factory = session_factory
        self._local = threading.local()
        self._sessions = {}
        self._lock = threading.Lock()

    def _current_session(self):
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._session_factory()
            self._local.session = session
            with self._lock:
                self._sessions[threading.get_ident()] = session
        return session

    @property
    def raw_connection(self):
        session = self._current_session()
        return getattr(session, "raw_connection", session)

    def cursor(self):
        return self._current_session().cursor()

    def execute(self, sql, params=None):
        return self._current_session().execute(sql, params)

    def executemany(self, sql, params):
        return self._current_session().executemany(sql, params)

    def fetchall(self):
        return self._current_session().fetchall()

    def fetchone(self):
        return self._current_session().fetchone()

    def commit(self):
        return self._current_session().commit()

    def rollback(self):
        return self._current_session().rollback()

    def autocommit(self, enabled):
        return self._current_session().autocommit(enabled)

    def close(self):
        session = getattr(self._local, "session", None)
        if session is None:
            return None
        try:
            return session.close()
        finally:
            with self._lock:
                self._sessions.pop(threading.get_ident(), None)
            self._local.session = None

    def close_all(self):
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            try:
                session.close()
            except Exception:
                pass
        self._local.session = None

    def __getattr__(self, item):
        return getattr(self._current_session(), item)
