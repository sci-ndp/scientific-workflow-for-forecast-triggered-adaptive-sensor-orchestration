"""Shared state tracking for BeeAI MCP modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass
class MCPStreamState:
    """Stores cached datasets, streams, and context for MCP-driven workflows."""

    producers: Dict[str, Any] = field(default_factory=dict)
    consumers: Dict[str, Any] = field(default_factory=dict)
    cached_search_results: Dict[Tuple[str, ...], Dict[str, Any]] = field(default_factory=dict)
    last_search_terms: Optional[List[str]] = None
    last_search_key: Optional[Tuple[str, ...]] = None
    registered_datasets: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    registered_datasets_by_id: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    last_registered_dataset: Optional[str] = None
    producer_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    last_created_topic: Optional[str] = None
    last_created_resource_ids: List[str] = field(default_factory=list)
    last_consumed_topic: Optional[str] = None

    @staticmethod
    def _normalize_terms(terms: Optional[Iterable[str]]) -> Optional[Tuple[str, ...]]:
        if not terms:
            return None
        normalized = sorted(
            {
                str(term).strip().lower()
                for term in terms
                if isinstance(term, str) and str(term).strip()
            }
        )
        return tuple(normalized) if normalized else None

    def record_search(self, keywords: List[str], results: List[Dict[str, Any]]) -> None:
        key = self._normalize_terms(keywords)
        if key is None:
            return
        self.cached_search_results[key] = {"keywords": keywords, "results": results}
        self.last_search_terms = keywords
        self.last_search_key = key

    def record_dataset(self, dataset: Dict[str, Any]) -> None:
        if not isinstance(dataset, dict):
            return
        name = dataset.get("name")
        dataset_id = dataset.get("id")
        if not name:
            if dataset_id and dataset_id in self.registered_datasets_by_id:
                name = self.registered_datasets_by_id[dataset_id].get("name")
            if not name:
                return
        self.registered_datasets[name] = dataset
        if dataset_id:
            self.registered_datasets_by_id[str(dataset_id)] = dataset
        self.last_registered_dataset = name

    def record_producer(
        self,
        topic: str,
        producer: Any,
        *,
        resource_ids: List[str],
        filter_semantics: Optional[List[str]] = None,
        filter_details: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.producers[topic] = producer
        self.producer_metadata[topic] = {
            "resource_ids": list(resource_ids),
            "filter_semantics": list(filter_semantics or []),
            "filter_details": list(filter_details or []),
        }
        self.last_created_topic = topic
        self.last_created_resource_ids = list(resource_ids)

    def forget_producer(self, topic: str) -> None:
        self.producers.pop(topic, None)
        self.producer_metadata.pop(topic, None)
        if self.last_created_topic == topic:
            self.last_created_topic = None
            self.last_created_resource_ids = []

    def record_consumer_start(self, topic: str, consumer: Any) -> None:
        self.consumers[topic] = consumer
        self.last_consumed_topic = topic

    def record_consumer_stop(self, topic: str) -> None:
        self.consumers.pop(topic, None)
        if self.last_consumed_topic == topic:
            self.last_consumed_topic = None

    @staticmethod
    def _extract_resource_ids(results: Optional[List[Dict[str, Any]]]) -> List[str]:
        resource_ids: List[str] = []
        for dataset in results or []:
            for resource in dataset.get("resources", []) or []:
                resource_id = resource.get("id")
                if resource_id:
                    resource_ids.append(str(resource_id))
        return resource_ids

    def get_resource_ids_from_search(self, keywords: Optional[List[str]] = None) -> List[str]:
        key = self._normalize_terms(keywords) if keywords else self.last_search_key
        if key is None:
            return []
        payload = self.cached_search_results.get(key)
        if not payload:
            return []
        return self._extract_resource_ids(payload.get("results"))

    def get_resource_ids_from_datasets(self, dataset_names: Optional[List[str]] = None) -> List[str]:
        names = dataset_names or []
        if not names and self.last_registered_dataset:
            names = [self.last_registered_dataset]
        resource_ids: List[str] = []
        for name in names:
            dataset = self.registered_datasets.get(name)
            if dataset:
                resource_ids.extend(self._extract_resource_ids([dataset]))
        return resource_ids

    def resolve_dataset_name(self, identifier: Optional[str]) -> Optional[str]:
        if not identifier:
            return None
        ident = identifier.strip()
        if not ident:
            return None
        if ident in self.registered_datasets:
            return ident
        dataset = self.registered_datasets_by_id.get(ident) or self.registered_datasets_by_id.get(str(ident))
        if dataset and dataset.get("name"):
            return dataset["name"]
        return None

    def resolve_dataset_id(self, identifier: Optional[str]) -> Optional[str]:
        if not identifier:
            return None
        ident = identifier.strip()
        if not ident:
            return None
        ident_str = str(ident)
        if ident_str in self.registered_datasets_by_id:
            return ident_str
        dataset = self.registered_datasets.get(ident_str) or self.registered_datasets.get(ident)
        if dataset and dataset.get("id"):
            return str(dataset["id"])
        return None

    def forget_dataset(self, identifier: Optional[str]) -> None:
        if not identifier:
            return
        ident = identifier.strip()
        if not ident:
            return

        ident_str = str(ident)
        dataset: Optional[Dict[str, Any]] = None

        if ident in self.registered_datasets:
            dataset = self.registered_datasets.pop(ident, None)
        elif ident_str in self.registered_datasets_by_id:
            dataset = self.registered_datasets_by_id.pop(ident_str, None)
        else:
            dataset = self.registered_datasets_by_id.pop(ident_str, None)

        if dataset:
            dataset_id = dataset.get("id")
            dataset_name = dataset.get("name")
            if dataset_id:
                self.registered_datasets_by_id.pop(str(dataset_id), None)
            if dataset_name:
                self.registered_datasets.pop(dataset_name, None)
        else:
            self.registered_datasets.pop(ident, None)
            self.registered_datasets_by_id.pop(ident_str, None)

        if self.last_registered_dataset and self.last_registered_dataset not in self.registered_datasets:
            self.last_registered_dataset = next(iter(self.registered_datasets), None)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "last_search_terms": self.last_search_terms,
            "cached_search_queries": [list(key) for key in self.cached_search_results.keys()],
            "last_registered_dataset": self.last_registered_dataset,
            "registered_dataset_names": sorted(self.registered_datasets.keys()),
            "active_streams": sorted(self.producers.keys()),
            "producer_metadata": self.producer_metadata,
            "active_consumers": sorted(self.consumers.keys()),
            "last_created_topic": self.last_created_topic,
            "last_created_resource_ids": self.last_created_resource_ids,
            "last_consumed_topic": self.last_consumed_topic,
        }

    def stop_all_consumers(self) -> List[str]:
        topics: List[str] = []
        for topic, consumer in list(self.consumers.items()):
            topics.append(topic)
            try:
                consumer.stop()
            except Exception:
                pass
        self.consumers.clear()
        self.last_consumed_topic = None
        return topics

    async def stop_all_producers(self) -> List[str]:
        topics: List[str] = []
        for topic, producer in list(self.producers.items()):
            topics.append(topic)
            try:
                await producer.stop()
            except Exception:
                pass
        self.producers.clear()
        self.producer_metadata.clear()
        self.last_created_topic = None
        self.last_created_resource_ids = []
        return topics


__all__ = ["MCPStreamState"]
