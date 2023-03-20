from dataclasses import dataclass, fields


@dataclass
class Color:
    """ Applied standard colors for use when printing to the terminal """
    reset: str = '\033[0m'
    # Foreground colors
    fg_black: str = '\033[30m'
    fg_red: str = '\033[31m'
    fg_green: str = '\033[32m'
    fg_yellow: str = '\033[33m'
    fg_blue: str = '\033[34m'
    fg_magenta: str = '\033[35m'
    fg_cyan: str = '\033[36m'
    fg_white: str = '\033[37m'
    fg_grey: str = '\033[38;5;8m'
    # Background colors
    bg_black: str = '\033[40m'
    bg_red: str = '\033[41m'
    bg_green: str = '\033[42m'
    bg_yellow: str = '\033[43m'
    bg_blue: str = '\033[44m'
    bg_magenta: str = '\033[45m'
    bg_cyan: str = '\033[46m'
    bg_white: str = '\033[47m'
    bg_grey: str = '\033[48;5;8m'

    def __getattr__(self, item):
        expected_fields = [f.name for f in fields(self)]
        if item not in expected_fields:
            raise AttributeError(f'Provided color does not exist: {item}')
        return getattr(self, item)


@dataclass
class Symbol:
    arrow_r: str = '→'
    arrow_l: str = '←'
    line: str = '━'
    success: str = '✅'
    warning: str = '⚠️'
    fail: str = '🚫'
    sep: str = '║'


def color_print(*args, fg=None, bg=None):
    """ Prints arguments to the terminal, in the specified foreground/background color.

    Args:
        fg (str): The foreground color to print
        bg (str): The background color to print
    """
    if not fg and not bg:
        print(*args)
        return None

    c = Color()
    style = ''.join([getattr(c, f'{k}_{v}') for k, v in {'fg': fg, 'bg': bg}.items() if v])
    print(f'{style}{" ".join(args)}{c.reset}')


if __name__ == '__main__':
    c = Color()
    s = Symbol()
    print(s)
    print(f'{s.arrow_r} {c.fg_red}This is fg_red{c.reset}')
    color_print(f'{s.line} This is fg_green', fg='green')
    color_print('This is fg_grey', fg='grey')
    color_print('This is fg_cyan', fg='cyan')
    color_print('This is bg_blue', bg='blue')
    color_print('This is fg_red and bg_blue', fg='red', bg='blue')
    print(f'This {s.sep} is normal')
    # color_print('This is wont print', color='bad_color')
    print(f'{c.bad_color}This is wont print{c.reset}')
