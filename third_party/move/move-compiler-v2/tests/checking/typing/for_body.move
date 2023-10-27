module 0x8675309::M {
    fun t0(init: u8, cond: bool) {
        for (i = init; cond; i = i + 1) ();
        for (i = init; cond; i = i + 1) (());
        for (i = init; cond; i = i + 1) {};
        for (i = init; cond; i = i + 1) { let x = 0; x; };
        for (i = init; cond; i = i + 1) { if (cond) () };
        for (i = init; cond; i = i + 1) { break };
    }
}
