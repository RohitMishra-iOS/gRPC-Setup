// Stand-in for a Swift app: the function carries a Swift mangled linkage name, so the server's
// demangler is exercised with real DWARF line tables.
int global_value;

__attribute__((noinline))
void add_to_cart(int quantity) __asm__("_$s20ProductCardAnimation14ViewControllerC9addToCart8quantityySi_tF");
void add_to_cart(int quantity) {
    global_value = quantity;
    int *bad = 0;
    *bad = quantity;        // line 11: the crash
}

int main(void) {
    add_to_cart(3);         // line 15
    return 0;
}
