from .worthing import fetch_payments as worthing
from .york import fetch_payments as york
from .bristol import fetch_payments as bristol
from .blaby import fetch_payments as blaby
from .newcastle import fetch_payments as newcastle
from .stockton import fetch_payments as stockton
from .east_hampshire import fetch_payments as east_hampshire
from .stevenage import fetch_payments as stevenage
from .durham import fetch_payments as durham

FETCHERS = {
    "Worthing": worthing,
    "City of York": york,
    "Bristol": bristol,
    "Blaby": blaby,
    "Newcastle": newcastle,
    "Stockton-on-Tees": stockton,
    "East Hampshire": east_hampshire,
    "Stevenage": stevenage,
    "Durham": durham
}
