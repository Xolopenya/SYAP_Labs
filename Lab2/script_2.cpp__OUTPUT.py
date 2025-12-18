


def main():
    n = 10
    x = 0

    for i in range(n):
        print("i=", i, " x=", x)

        if i % 2 == 0:
            x += 1
        else:
            x -= 1

    print("final x=", x)
    return

if __name__ == '__main__':
    main()
