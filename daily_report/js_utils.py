def get_chars_counts(s):
    """返回出现次数最多的字符以及该字符出现的所有位置"""
    chars_index = dict()
    for index, char in enumerate(s):
        if char in chars_index:
            chars_index[char].append(index)
        else:
            chars_index[char] = [index]
    key_val_list = sorted(chars_index.items(), key=lambda val: len(val[1]), reverse=True)
    character = key_val_list[0][0]
    index = key_val_list[0][1]
    print(f'character: {character} \nindex:{index}')
    return character, index


if __name__ == '__main__':
    s = '231abca'
    get_chars_counts(s)
