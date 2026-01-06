// The following double values are used for double on-disk serialization, and
// are initialized at runtime to avoid strange compiler optimizations.
pub const Zero: f64 = 0.0;
pub const PosInf: f64 = std.math.inf(f64);
pub const NegInf: f64 = -PosInf;
pub const Nan: f64 = Zero / Zero;

const std = @import("std");
