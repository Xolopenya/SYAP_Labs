#include <iostream>
using namespace std;

int main() {
    int a;
    double price = 19.99;
    bool ok = true;
    string name = "lab2";

    cout << "Enter integer a:" << endl;
    cin >> a;

    cout << "a=" << a << " price=" << price << " ok=" << ok << " name=" << name << endl;

    a = a + 10;
    cout << "a after +10 = " << a << endl;

    return 0;
}
