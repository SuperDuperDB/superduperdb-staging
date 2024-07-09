import typing as t


class LocalSequentialQueue:
    def __init__(self):
        self.queue = {}
        self.components= {}
        self._db = None
        self._component_map = {}

    def declare_component(self, component):
        identifier =f'{component.type_id}.{component.identifier}' 
        self.queue[identifier] = []
        self.components[identifier] = component

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, db):
        self._db = db

    def publish(self, events: t.List[t.Dict] , to: t.Dict[str, str]):
        identifier = to['identifier']
        type_id = to['type_id']
        self._component_map.update(to)
        
        self.queue[f'{type_id}.{identifier}'].extend(events)
        return self.consume()

    def consume(self):
        from superduperdb.base.datalayer import Event
        queue_jobs = {}
        for component in self.queue:
            events  = self.queue[component]
            if not events:
                continue
            self.queue[component] = []

            component = self.components[component]
            jobs = []
            
            for event_type, type_events in Event.chunk_by_event(events).items():
                ids = [event['identifier'] for event in type_events]
                overwrite = True if event_type in [Event.insert, Event.upsert] else False
                job = component.run_jobs(db=self.db, ids=ids, overwrite=overwrite)
                jobs.append(job)
            
            if component in queue_jobs:
                queue_jobs[component].extend(jobs)
            else:
                queue_jobs[component] = jobs

        return queue_jobs
