run-less:
  [ ! -f /danny.out ] || rm /tmp/danny.out
  cargo run > /tmp/danny.out || true
  less /tmp/danny.out

