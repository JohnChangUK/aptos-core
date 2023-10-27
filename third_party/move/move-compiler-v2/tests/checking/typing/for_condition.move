module 0x8675309::M {
    fun t0(init: u8) {
        for (i = init; true; i = i + 1) ();
        for (i = init; false; i = i + 1) ();
    }

    fun t1(init: u8) {
        for (i = init; { let foo = true; foo }; i = i + 1) ();
        for (i = init; { let bar = false; bar }; i = i + 1) ();
    }
}
