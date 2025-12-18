#include <iostream>
using namespace std;

int main() {
    int n = 10;
    int x = 0;

    for (int i = 0; i < n; i++) {
        cout << "i=" << i << " x=" << x << endl;

        if (i % 2 == 0) {
            x++;
        }
        else {
            x--;
        }
    }

    cout << "final x=" << x << endl;
    return 0;
}
