#!/usr/bin/env python3
"""
Greeting text.

"""

reset = "\u001b[0m"
blue = "\u001b[38;5;61m"
green = "\u001b[38;5;107m"
orange = "\u001b[38;5;209m"

greeting_artwork = """
  {}){}{}a,{}
  {})/{}{}#L,{}                {}┌─┐┬  ┌─┐┌┬┐┌┬┐{}
  {}v({}{}"###a{}              {}├─┘│  ├─┤ │  │{}
  {}vv,{}{}"####a,{}           {}┴  ┴─┘┴ ┴ ┴  ┴{}
  {}vv({} {}4#####L,{}         {}┌─┐┌─┐┌─┐┬ ┬{}
 {}=vvv{}  {}!!4#####a{}       {}│  ├┤ ├─┘├─┤{}
 {}=vv>{}       {}!!!##a,.{}   {}└─┘└─┘┴  ┴ ┴{}
 {}=v>{} {}sXXXXXXXsssssZP*{}  {}┌─┐┌─┐┌┬┐┌─┐┬ ┬┌─┐┬ ┬{}
 {}%>{}{}_XXXXXXXX7\"\"\"\"{}  {}    │ ┬├─┤ │ ├┤ │││├─┤└┬┘{}
 {}v{}{}J7\"\"\"\"{}           {}    └─┘┴ ┴ ┴ └─┘└┴┘┴ ┴ ┴{}
""".format(
    blue, reset, orange, reset,
    blue, reset, orange, reset, orange, reset,
    blue, reset, orange, reset, orange, reset,
    blue, reset, orange, reset, orange, reset,
    blue, reset, orange, reset, blue,   reset,
    blue, reset, orange, reset, blue,   reset,
    blue, reset, orange, reset, blue,   reset,
    blue, reset, green,  reset, green,  reset,
    blue, reset, green,  reset, green,  reset,
    blue, reset, green,  reset, green,  reset
)
