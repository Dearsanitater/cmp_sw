from __future__ import annotations

from django.http import JsonResponse


def comp(request):
    return JsonResponse(
        {
            "status": "disabled",
            "message": "direct compare endpoint is disabled; create a rule job from /rule instead",
        },
        status=410,
    )


def ajaxGetTxt(request):
    return JsonResponse(
        {
            "status": "disabled",
            "message": "report text endpoint is disabled in control-plane mode",
        },
        status=410,
    )
