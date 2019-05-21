#!/usr/bin/env python3
"""
Greeting text. A map of East Frisia.

"""

reset = "\u001b[0m"
deep_blue = "\u001b[38;5;18m"
blue = "\u001b[38;5;45m"
green = "\u001b[38;5;77m"
red = "\u001b[38;5;124m"

greet = [
    "{}   oooOOOQ{}QOOO{}QOo{}QO{}a/{}Oo{}oo,{}".format(deep_blue, blue, deep_blue, blue, green, blue, deep_blue, reset),
    "{}  oOOQQ{}QQQ{}--{}QQ{}?\"'{}QQ{}aa/{}P*{}".format(deep_blue, blue, green, blue, green, blue, green, blue, reset),
    "{} oQQ{}Q{}\"r{}QQQ{}_wQQOQQQOQQ@{}      +----------------------------------+".format(deep_blue, blue, green, blue, green, reset),
    "{}o{}O{}\"?\"{}QQQQQ{}QQQQQQOOO{}P{}QQ{}      |                                  |".format(deep_blue, blue, green, blue, green, red, green, reset),
    "{}  QOo{}oQQ{}/ajQQQQ{}o{}QQQQQga/{}    |  This is the platt proxy server  |".format(deep_blue, blue, green, red, green, reset),
    "{}    QOoo{}jQQQQQQQQQQQQQQP{}    |                                  |".format(blue, green, reset),
    "{}     QQQ{}QQ{}Oo{}QQQQQQQQQQ/{}     |   Connect the platt backend to   |".format(blue, green, red, green, reset),
    "{}       QQQQQ{}wQQWQQQQQQ'{}     |     receive simulation data.     |".format(blue, green, reset),
    "{}         QQ{}_QQQ{}W{}QQQQ?{}       |                                  |".format(blue, green, red, green, reset),
    "{}         QQ{}jQQmQQQP'{}        +----------------------------------+".format(blue, green, reset),
    "{}           {}???4WWQ'{}".format(blue, green, reset),
    "{}                {})?{}".format(blue, green, reset)
]

greeting = "\n"

for g in greet:
    greeting += "{}\n".format(g)
