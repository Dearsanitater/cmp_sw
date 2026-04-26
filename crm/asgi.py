# asgi.py
import os
import pdb

from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from django.urls import re_path
from crm.consumers import MessageConsumer
from django.core.asgi import get_asgi_application
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'crm.settings')

print(f'asgi{os.getcwd()}')

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": AuthMiddlewareStack(
        URLRouter([
            re_path(r'ws/message/$', MessageConsumer.as_asgi()),  # 所有 WebSocket 连接都通过这个路由
        ])
    ),
})
print(f'asgi2{os.getcwd()}')