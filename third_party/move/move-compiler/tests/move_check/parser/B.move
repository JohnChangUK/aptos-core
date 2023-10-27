module 0x11eee::B {
    public fun for_loop() {
        for (i in 5..9) {
            if (i == 3) {
                break
            } else {
                continue
            };
        };
    }
}
