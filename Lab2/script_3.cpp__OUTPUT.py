


def demoLongFunction():
    n = 0
    total = 0
    positives = 0
    negatives = 0

    avg = 0.0
    hasZero = False
    label = "demo"

    print("Enter n (amount of numbers):")
    n = int(input())

    if n <= 0:
        print("n must be positive")
        return

    for i in range(n):
        x = 0
        print("Enter x:")
        x = int(input())

        print("i=", i, " x=", x)

        total = total + x

        if x > 0:
            positives += 1
            print("positive count=", positives)
        else:
            if x < 0:
                negatives += 1
                print("negative count=", negatives)
            else:
                hasZero = True
                print("zero detected")

        if i % 2 == 0:
            print("even i -> total=", total)
        else:
            print("odd i -> total=", total)

    avg = total / float(n)

    print("label=", label)
    print("total=", total)
    print("avg=", avg)
    print("positives=", positives)
    print("negatives=", negatives)
    print("hasZero=", hasZero)

    if avg > 0.0 and not hasZero:
        print("avg positive and no zeros")
    else:
        print("avg not positive or there were zeros")

    return total

def main():
    demoLongFunction()
    return

if __name__ == '__main__':
    main()
