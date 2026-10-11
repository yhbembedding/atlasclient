"""atlasclient: gọi agent, team, workflow của Atlas từ ứng dụng khác (chatbot, backend, job…).

    from atlasclient import Atlas

    atlas = Atlas("https://atlas.example.com", token=user_jwt)
    chat = atlas.chat("troluy")
    for event in chat.stream("xin chào"):
        if event.type == "text":
            print(event.text, end="")
"""

from atlasclient.aio import AsyncAtlas, AsyncConversation
from atlasclient.client import Atlas, Conversation
from atlasclient.errors import (
    APIConnectionError,
    APIStatusError,
    AtlasError,
    AuthenticationError,
    BudgetExceeded,
    Conflict,
    ConversationBusy,
    InvalidRequest,
    NotFound,
    PayloadTooLarge,
    PermissionDenied,
    RateLimited,
    RunTimeout,
    ServerError,
    StreamError,
    UnsupportedMediaType,
)
from atlasclient.events import (
    ApprovalRequired,
    ChatEvent,
    ChatEventMapper,
    Done,
    Failed,
    MemberFinished,
    MemberStarted,
    RunEvent,
    TextDelta,
    TextRetracted,
    Thinking,
    ToolFinished,
    ToolStarted,
    parse_event,
)
from atlasclient.types import (
    AgentInfo,
    Decision,
    Identity,
    Media,
    Message,
    PendingKind,
    PendingToolCall,
    Run,
    RunStatus,
    Session,
    SessionDetail,
    SessionInfo,
    ToolCall,
    Usage,
    WorkflowInfo,
)

__version__ = "0.1.0"

__all__ = [
    "APIConnectionError", "APIStatusError", "AgentInfo", "ApprovalRequired", "AsyncAtlas", "AsyncConversation",
    "Atlas", "AtlasError", "AuthenticationError", "BudgetExceeded", "ChatEvent", "ChatEventMapper", "Conflict",
    "Conversation", "ConversationBusy", "Decision", "Done", "Failed", "Identity", "InvalidRequest", "Media",
    "MemberFinished", "MemberStarted", "Message", "NotFound", "PayloadTooLarge", "PendingKind", "PendingToolCall",
    "PermissionDenied", "RateLimited", "Run", "RunEvent", "RunStatus", "RunTimeout", "ServerError", "Session",
    "SessionDetail", "SessionInfo", "StreamError", "TextDelta", "TextRetracted", "Thinking", "ToolCall",
    "ToolFinished", "ToolStarted", "UnsupportedMediaType", "Usage", "WorkflowInfo", "parse_event",
]
