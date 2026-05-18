import asyncio
import os
import re
import threading
from typing import Optional, TYPE_CHECKING, Dict, Any, List, Iterable

import jwt
import logging
import warnings
from ndp_ep import APIClient

os.environ["GRPC_PYTHON_LOG_SEVERITY"] = "FATAL"
os.environ["GRPC_PYTHON_LOG_LEVEL"] = "ERROR"
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_TRACE", "")
os.environ.setdefault("GLOG_minloglevel", "3")
os.environ.setdefault("ABSL_LOG_LEVEL", "3")
os.environ.setdefault("ABSL_LOGLEVEL", "3")
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")

try:  # pragma: no cover - optional dependency
    from absl import logging as absl_logging

    absl_logging.set_verbosity(absl_logging.ERROR)
    absl_logging.set_stderrthreshold("fatal")
except Exception:  # pragma: no cover
    pass

from ._mcp_config import ModuleConfiguration, derive_module_configuration
from .delete_resource import StreamingResourceDeletion
from .registration import StreamingDataSourceRegistration
from .search_consumption_methods import StreamingDataSourceSearch

if TYPE_CHECKING:  # pragma: no cover
    from .mcp import StreamingClientMCPAdapter

logger = logging.getLogger(__name__)


def _import_genai():
    """Import the deprecated Gemini SDK without surfacing its package warning."""

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="All support for the `google.generativeai` package has ended.*",
            category=FutureWarning,
        )
        from google import generativeai as genai

    return genai

class StreamingClient(APIClient, StreamingDataSourceRegistration, StreamingDataSourceSearch, StreamingResourceDeletion):
    """
    A client to interact with the Streaming API and integrate with the PointOfPresence API.

    This class extends APIClient, inheriting its methods.

    Parameters
    ----------
    pop_client : APIClient
        An instance of the PointOfPresence API client.

    Attributes
    ----------
    base_url : str
        The base URL of the Streaming API.
    token : str
        The authentication token.
    user_id : str
        The extracted user ID from the token.
    KAFKA_HOST : str
        Kafka host retrieved from POP API connection details.
    KAFKA_PORT : int
        Kafka port retrieved from POP API connection details.
    KAFKA_PREFIX : str
        The prefix used for Kafka streams.
    MAX_STREAMS : int
        The maximum number of streams allowed.
    """

    def __init__(
        self,
        pop_client: APIClient,
        *,
        mcp_provider: Optional[str] = None,
        api_key: Optional[str] = None,
        mcp_model: Optional[str] = None,
        mcp_system_prompt: Optional[str] = None,
        mcp_max_history: int = 8,
        model: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> None:
        """
        Initialize the StreamingClient with an existing PointOfPresence APIClient.

        Parameters
        ----------
        pop_client : APIClient
            An existing APIClient instance.
        mcp_provider : str, optional
            Shorthand to configure the MCP adapter on creation (e.g., "gemini", "groq").
        api_key : str, optional
            API key for the chosen MCP provider.
        mcp_model / model / model_name : str, optional
            Preferred model identifier for the provider. ``model`` and ``model_name`` are
            aliases for ``mcp_model`` to support simpler call sites.
        mcp_system_prompt : str, optional
            Override system prompt for MCP interactions.
        mcp_max_history : int, optional
            Number of turns to include from prior conversations when prompting the LLM.
        """
        if not isinstance(pop_client, APIClient):
            raise ValueError("`pop_client` must be an instance of APIClient.")

        # Initialize the parent class
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Could not determine API version from status endpoint",
            )
            super().__init__(
                base_url=pop_client.base_url,
                token=pop_client.token,
                username=None,
                password=None,
            )

        self.base_url = pop_client.base_url
        self.session = pop_client.session
        self.token = pop_client.token

        # Kafka configurations with defaults
        self.KAFKA_HOST = None
        self.KAFKA_PORT = None
        self.KAFKA_PREFIX = "data_stream_"
        self.MAX_STREAMS = 10

        # Decode token to set user ID
        self.user_id = self._decode_user_id()

        # Fetch Kafka details
        self._fetch_kafka_details()

        # Adapter for BeeAI/Gemini integrations (lazy instantiation)
        self._mcp_adapter: Optional["StreamingClientMCPAdapter"] = None
        self._mcp_provider: Optional[str] = None
        selected_model = mcp_model or model or model_name
        self._mcp_config: Dict[str, Any] = {
            "profile": None,
            "model": selected_model,
            "system_prompt": mcp_system_prompt,
            "api_key": api_key,
            "max_history": mcp_max_history,
            "api_base": None,
            "temperature": None,
            "max_output_tokens": None,
            "request_timeout": None,
            "python_executable": None,
            "modules": None,
            "disable_modules": None,
        }
        self._mcp_client = None
        self._mcp_loop: Optional[asyncio.AbstractEventLoop] = None
        self._mcp_loop_thread: Optional[threading.Thread] = None
        self._mcp_connection_history: List[Dict[str, Any]] = []

        if mcp_provider or api_key or selected_model or mcp_system_prompt:
            self.configure_mcp(
                provider=mcp_provider,
                api_key=api_key,
                model=selected_model,
                model_name=model_name,
                system_prompt=mcp_system_prompt,
                max_history=mcp_max_history,
            )

    def get_mcp_adapter(self) -> "StreamingClientMCPAdapter":
        """Return the cached MCP adapter, creating it if necessary."""

        if self._mcp_adapter is None:
            from .mcp import StreamingClientMCPAdapter

            self._mcp_adapter = StreamingClientMCPAdapter(self)
        return self._mcp_adapter

    @property
    def mcp(self) -> "StreamingClientMCPAdapter":
        """Shortcut property to access MCP utilities."""

        return self.get_mcp_adapter()

    def describe_mcp_services(self) -> List[Dict[str, Any]]:
        """Return the BeeAI MCP service catalog for this client."""

        adapter = self.get_mcp_adapter()
        return adapter.describe_services()

    # ------------------------------------------------------------------
    # MCP high-level helpers
    # ------------------------------------------------------------------

    def configure_mcp(
        self,
        *,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        model_name: Optional[str] = None,
        system_prompt: Optional[str] = None,
        max_history: Optional[int] = None,
        api_base: Optional[str] = None,
        temperature: Optional[float] = None,
        max_output_tokens: Optional[int] = None,
        request_timeout: Optional[float] = None,
        modules: Optional[Iterable[str] | str] = None,
        disable_modules: Optional[Iterable[str] | str] = None,
        **_: Any,
    ) -> None:
        """Configure the MCP provider parameters for this client instance."""

        adapter = self.get_mcp_adapter()
        module_configuration = derive_module_configuration(modules, disable_modules)

        self._apply_module_configuration(module_configuration, adapter)

        if provider:
            normalized = str(provider).strip().lower()
            if normalized in {"gemini", "geminis", "google-gemini"}:
                self._mcp_provider = "gemini"
            elif normalized in {"groq"}:
                self._mcp_provider = "groq"
            elif normalized in {"beeai", "bee-ai", "beeai-default"}:
                self._mcp_provider = "beeai"
            else:
                raise ValueError(
                    f"Unsupported MCP provider '{provider}'. Supported providers: beeai, gemini, groq."
                )

        profile_name: Optional[str] = None
        resolved_model: Optional[str] = None

        candidate_model = model or model_name
        if candidate_model:
            candidate = str(candidate_model).strip()
            if candidate:
                profile = adapter.get_profile(candidate)
                if profile:
                    profile_name = profile.name
                    if not self._mcp_provider:
                        self._mcp_provider = profile.provider.strip().lower()
                else:
                    resolved_model = candidate

        if api_key is not None:
            self._mcp_config["api_key"] = api_key
        if profile_name is not None:
            self._mcp_config["profile"] = profile_name
            # Profile implies default model unless overridden explicitly.
            if resolved_model is None:
                self._mcp_config["model"] = None
        else:
            self._mcp_config["profile"] = None
        if resolved_model:
            self._mcp_config["model"] = resolved_model
        if system_prompt is not None:
            self._mcp_config["system_prompt"] = system_prompt
        if max_history is not None:
            self._mcp_config["max_history"] = max(1, int(max_history))
        if api_base is not None:
            self._mcp_config["api_base"] = api_base
        if temperature is not None:
            self._mcp_config["temperature"] = float(temperature)
        if max_output_tokens is not None:
            self._mcp_config["max_output_tokens"] = int(max_output_tokens)
        if request_timeout is not None:
            self._mcp_config["request_timeout"] = float(request_timeout)

        # If we already have an active MCP client, close it so the next call rebuilds with new settings.
        if self._mcp_client is not None:
            self._run_coroutine_sync(self._close_mcp_client())

    def _apply_module_configuration(
        self,
        module_configuration: ModuleConfiguration,
        adapter: "StreamingClientMCPAdapter",
    ) -> None:
        """Persist module selections on both the client and MCP adapter."""

        if module_configuration.remote is not None:
            self._mcp_config["remote_modules"] = module_configuration.remote
        else:
            self._mcp_config.pop("remote_modules", None)

        if module_configuration.enabled is not None:
            self._mcp_config["modules"] = list(module_configuration.enabled)
        else:
            self._mcp_config.pop("modules", None)

        if module_configuration.disabled is not None:
            self._mcp_config["disable_modules"] = list(module_configuration.disabled)
        else:
            self._mcp_config.pop("disable_modules", None)

        if module_configuration.enabled is not None or module_configuration.disabled is not None:
            adapter.configure_modules(module_configuration.enabled, module_configuration.disabled)

        adapter.configure_remote_modules(module_configuration.remote)

    def ask_mcp(self, prompt: str) -> str:
        """Synchronously execute an MCP interaction using the configured provider."""

        return self._run_coroutine_sync(self.ask_mcp_async(prompt))

    async def ask_mcp_async(self, prompt: str) -> str:
        """Async variant of :meth:`ask_mcp`. Returns the assistant response string."""

        client = await self._ensure_mcp_client()
        try:
            return await client.chat(prompt)
        except Exception:
            await self._close_mcp_client()
            raise

    def close_mcp(self) -> None:
        """Synchronously close any active MCP connection."""

        self._run_coroutine_sync(self._close_mcp_client())
        self._stop_async_loop()

    async def close_mcp_async(self) -> None:
        """Async helper to close the MCP connection."""

        await self._close_mcp_client()
        self._stop_async_loop()

    def _decode_user_id(self):
        """
        Decode the token to extract the user ID.

        Returns
        -------
        str
            The extracted user ID from the token.
        """
        try:
            decoded_payload = jwt.decode(self.token, options={"verify_signature": False})
            user_id = decoded_payload.get("sub")
            if not user_id:
                raise ValueError("User ID not found in token.")
            logger.info(f"Extracted user ID: {user_id}")
            return user_id
        except jwt.DecodeError as e:
            logger.error(f"Error decoding token: {e}")
            raise ValueError("Invalid token provided.")

    def _fetch_kafka_details(self):
        """
        Fetch Kafka connection details from the POP API and set them as attributes.
        """
        try:
            kafka_details = self.get_kafka_details()
            if kafka_details.get("kafka_connection"):
                self.KAFKA_HOST = kafka_details["kafka_host"]
                self.KAFKA_PORT = kafka_details["kafka_port"]
                # Attempt to get the optional Kafka prefix and max streams
                self.KAFKA_PREFIX = kafka_details.get("kafka_prefix", self.KAFKA_PREFIX)
                self.MAX_STREAMS = kafka_details.get("max_streams", self.MAX_STREAMS)
                logger.info(f"Kafka details set: HOST={self.KAFKA_HOST}, PORT={self.KAFKA_PORT}, "
                            f"PREFIX={self.KAFKA_PREFIX}, MAX_STREAMS={self.MAX_STREAMS}")
            else:
                logger.warning("Kafka connection is not active. Streaming capabilities are disabled.")
                print("Warning: The Point of Presence is not configured with Kafka. Streaming capabilities are off.")
        except Exception as e:
            logger.error(f"Failed to fetch Kafka details. Streaming capabilities are disabled. Error: {e}")
            print("Error: Unable to fetch Kafka details. Streaming capabilities are off.")

    # ------------------------------------------------------------------
    # MCP internals
    # ------------------------------------------------------------------

    def _run_coroutine_sync(self, coro):
        loop = self._ensure_async_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    async def _ensure_mcp_client(self):
        if self._mcp_client is not None:
            return self._mcp_client

        adapter = self.get_mcp_adapter()
        profile_name = self._mcp_config.get("profile")
        profile = adapter.get_profile(profile_name) if profile_name else None
        provider = (self._mcp_provider or (profile.provider if profile else None))
        if not provider:
            raise RuntimeError(
                "MCP provider not configured. Call configure_mcp(...) or select a named profile."
            )
        provider = provider.strip().lower()

        config = dict(self._mcp_config)

        if provider == "gemini":
            if not config.get("api_key"):
                config["api_key"] = os.getenv("GEMINI_API_KEY")
            api_key = config.get("api_key")
            if not api_key:
                raise RuntimeError("Gemini API key not configured. Pass api_key=... or set GEMINI_API_KEY.")

            requested_model = config.get("model") or (profile.model if profile else None)
            config["model"] = self._resolve_model_name(api_key, requested_model)
            self._silence_grpc_logs()

        elif provider == "groq":
            if not config.get("api_key"):
                config["api_key"] = os.getenv("GROQ_API_KEY")
            if not config.get("api_key"):
                raise RuntimeError("Groq API key not configured. Pass api_key=... or set GROQ_API_KEY.")
            if not config.get("api_base"):
                config["api_base"] = os.getenv("GROQ_API_BASE")

        elif provider == "beeai":
            # No additional preparation required; BeeAI agent will resolve its own defaults.
            pass
        else:
            raise RuntimeError(f"Unsupported MCP provider '{provider}'.")

        extra_env = {
            "SCIDX_API_URL": self.base_url,
            "SCIDX_API_TOKEN": self.token,
        }

        enabled_modules, disabled_modules = adapter.module_filters()
        if enabled_modules:
            extra_env["SCIDX_MCP_MODULES"] = ",".join(enabled_modules)
        if disabled_modules:
            extra_env["SCIDX_MCP_DISABLE_MODULES"] = ",".join(disabled_modules)

        # LLM clients do not need module filter metadata.
        config.pop("modules", None)
        config.pop("disable_modules", None)

        client, python_exec = adapter.build_llm_client(
            profile_name,
            provider,
            config,
        )

        await client.connect(
            adapter.server_script_path(),
            extra_env=extra_env,
            python_executable=python_exec,
        )
        self._mcp_connection_history.append(
            {
                "provider": provider,
                "profile": profile_name,
                "model": config.get("model"),
                "options": {
                    key: config.get(key)
                    for key in ("api_base", "base_url", "temperature", "max_history")
                    if key in config and config.get(key) is not None
                },
            }
        )
        self._mcp_client = client
        return self._mcp_client

    async def _close_mcp_client(self):
        client = self._mcp_client
        if client is None:
            return

        try:
            await client.disconnect()
        finally:
            self._mcp_client = None
            if self._mcp_adapter is not None:
                try:
                    await self._mcp_adapter.shutdown()
                except Exception:
                    pass

    @staticmethod
    def _silence_grpc_logs() -> None:
        os.environ["GRPC_PYTHON_LOG_SEVERITY"] = "FATAL"
        os.environ["GRPC_PYTHON_LOG_LEVEL"] = "ERROR"
        os.environ.setdefault("GRPC_VERBOSITY", "NONE")
        os.environ.setdefault("GRPC_TRACE", "")

    def _ensure_async_loop(self) -> asyncio.AbstractEventLoop:
        loop = self._mcp_loop
        thread = self._mcp_loop_thread
        if loop and thread and thread.is_alive() and not loop.is_closed():
            return loop

        loop_ready = threading.Event()

        def _loop_runner():
            new_loop = asyncio.new_event_loop()
            self._mcp_loop = new_loop
            asyncio.set_event_loop(new_loop)
            loop_ready.set()
            try:
                new_loop.run_forever()
            finally:
                new_loop.run_until_complete(new_loop.shutdown_asyncgens())
                new_loop.close()

        thread = threading.Thread(target=_loop_runner, daemon=True)
        self._mcp_loop_thread = thread
        thread.start()
        loop_ready.wait()
        return self._mcp_loop

    def _stop_async_loop(self) -> None:
        loop = self._mcp_loop
        thread = self._mcp_loop_thread
        if not loop or not thread:
            return
        if loop.is_closed() or not thread.is_alive():
            self._mcp_loop = None
            self._mcp_loop_thread = None
            return

        loop.call_soon_threadsafe(loop.stop)
        if threading.current_thread() is not thread:
            thread.join(timeout=2)
        self._mcp_loop = None
        self._mcp_loop_thread = None

    def _resolve_model_name(self, api_key: str, requested: Optional[str]) -> str:
        """Pick a valid Gemini model name, falling back to available defaults."""

        genai = _import_genai()
        genai.configure(api_key=api_key)

        preferred_inputs = [requested, os.getenv("GEMINI_MODEL")]
        preferred = [c.strip() for c in preferred_inputs if c and c.strip()]

        available: List[str] = []
        try:
            available = [
                model.name
                for model in genai.list_models()
                if "generateContent" in getattr(model, "supported_generation_methods", [])
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("Unable to list Gemini models: %s", exc)

        def _first_match(candidates: List[str]) -> Optional[str]:
            for candidate in candidates:
                if candidate in available:
                    return candidate
            return None

        if preferred:
            matched = _first_match(preferred)
            if matched:
                logger.info("Using Gemini model '%s'", matched)
                return matched

        fallback_priority = [
            "models/gemini-2.5-pro",
            "models/gemini-2.5-flash",
            "models/gemini-2.5-flash-preview-05-20",
            "models/gemini-2.5-flash-lite",
            "models/gemini-2.0-flash-exp",
            "models/gemini-2.0-flash",
            "models/gemini-pro-latest",
            "models/gemini-flash-latest",
            "models/gemini-1.5-flash",
            "models/gemini-1.5-pro",
            "models/gemini-1.0-pro",
        ]
        matched = _first_match(fallback_priority)
        if matched:
            logger.info("Using Gemini model '%s'", matched)
            return matched

        if available:
            logger.info("Defaulting to first available Gemini model '%s'", available[0])
            return available[0]

        # As a final attempt, try instantiating preferred or fallback names even without list_models data.
        probe_candidates = preferred + fallback_priority
        errors: Dict[str, str] = {}
        for candidate in probe_candidates:
            if not candidate:
                continue
            try:
                genai.GenerativeModel(candidate).start_chat(history=[])  # probes generateContent support
                logger.info("Using Gemini model '%s'", candidate)
                return candidate
            except Exception as exc:  # noqa: BLE001
                errors[candidate] = str(exc)
                logger.debug("Gemini model probe failed for '%s': %s", candidate, exc)

        error_summary = "; ".join(f"{name}: {reason}" for name, reason in errors.items()) or "no candidates available"
        raise RuntimeError(
            "Unable to find a Gemini model that supports generateContent. "
            f"Checked: {', '.join(probe_candidates) or 'none'}. Errors: {error_summary}. "
            "Set GEMINI_MODEL or pass model=... to configure_mcp with a supported model name."
        )
