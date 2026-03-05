content = open('scanner.py', 'r', encoding='utf-8').read().splitlines()
for i in range(349, 360):
    print(f'{i+1}: {repr(content[i])}')
