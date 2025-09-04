from .worthing import fetch_payments as worthing
from .bristol import fetch_payments as bristol
from .newcastle import fetch_payments as newcastle
from .stockton import fetch_payments as stockton
from .east_hampshire import fetch_payments as east_hampshire
from .durham import fetch_payments as durham

FETCHERS = {
    "Worthing": worthing,
    "Bristol": bristol,
    "Newcastle": newcastle,
    "Stockton-on-Tees": stockton,
    "East Hampshire": east_hampshire,
    "Durham": durham
}
