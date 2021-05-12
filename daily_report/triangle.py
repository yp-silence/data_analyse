def print_triangle(rows):
    assert rows >= 2, "rows to short";
    for row in range(1, rows + 1):
        line_str = ''
        line_str += ' ' * (rows - row) + '*' * (2 * row - 1) + ' ' * (rows - row)
        print(line_str, end='\n')


if __name__ == '__main__':
    print_triangle(4)
