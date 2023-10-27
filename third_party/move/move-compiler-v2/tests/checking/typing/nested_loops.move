module 0x8675309::M {
    fun t0(w_cond: bool, iter: u8, f_cond: bool) {
        while (w_cond) { for (i = iter; f_cond; i = i + 1) {} };
    }

    fun t1(iter: u8, f_cond: bool, w_cond: bool) {
        for (i = iter; f_cond; i = i + 1) {while (w_cond) {} };
    }
}
