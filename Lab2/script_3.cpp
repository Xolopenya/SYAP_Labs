#include <iostream>
using namespace std;

int demoLongFunction() {
    int n;
    int total = 0;
    int positives = 0;
    int negatives = 0;

    double avg = 0.0;
    bool hasZero = false;
    string label = "demo";

    cout << "Enter n (amount of numbers):" << endl;
    cin >> n;

    if (n <= 0) {
        cout << "n must be positive" << endl;
        return 0;
    }

    for (int i = 0; i < n; i++) {
        int x;
        cout << "Enter x:" << endl;
        cin >> x;

        cout << "i=" << i << " x=" << x << endl;

        total = total + x;

        if (x > 0) {
            positives++;
            cout << "positive count=" << positives << endl;
        }
        else {
            if (x < 0) {
                negatives++;
                cout << "negative count=" << negatives << endl;
            }
            else {
                hasZero = true;
                cout << "zero detected" << endl;
            }
        }

        if (i % 2 == 0) {
            cout << "even i -> total=" << total << endl;
        }
        else {
            cout << "odd i -> total=" << total << endl;
        }
    }

    avg = total / (double)n;

    cout << "label=" << label << endl;
    cout << "total=" << total << endl;
    cout << "avg=" << avg << endl;
    cout << "positives=" << positives << endl;
    cout << "negatives=" << negatives << endl;
    cout << "hasZero=" << hasZero << endl;

    if (avg > 0.0 && !hasZero) {
        cout << "avg positive and no zeros" << endl;
    }
    else {
        cout << "avg not positive or there were zeros" << endl;
    }

    return total;
}

int main() {
    demoLongFunction();
    return 0;
}
