# Test fixtures

`ProductCardAnimation.app.dSYM.zip` — an arm64 iOS dSYM (UUID 4C4C443B-5555-3144-A1D3-448C2E620BD3)
built from `dsym_source_ViewController.c` (compiled as ViewController.c) with clang 18, `ld64.lld` and `dsymutil`:

    clang -target arm64-apple-ios15.0 -g -O0 -c dsym_source_ViewController.c -o ViewController.o
    ld64.lld -arch arm64 -platform_version ios 15.0 15.0 -e _main -o ProductCardAnimation ViewController.o
    dsymutil ProductCardAnimation -o ProductCardAnimation.app.dSYM

`add_to_cart` carries a Swift mangled name (`$s20ProductCardAnimation14ViewControllerC9addToCart8quantityySi_tF`),
so the server's Swift demangling and DWARF line lookup are tested together (tests/e2e_symbols.py):
0x100004018 is line 10 (the crash), 0x100004044 is the call on line 14.
