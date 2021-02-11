"""
Type convert function and value checker classes
"""
import abc
import logging


def convert_with_check(
        value,
        *,
        output_type,
        checker,
        replace_if_false=True,
        value_if_false=None,
        warn_if_false=False,
        replace_if_conversion_error=True,
        value_if_conversion_error=None,
        warn_if_conversion_error=False,
        replace_if_checker_error=True,
        value_if_checker_error=None,
        warn_if_checker_error=False,
        info=None
):
    """
    Convert the value to output_type, and check it using checker
    :param value: the value to be converted
    :param output_type: the new type of the value
    :param checker: the checker instance (raises ValueError if check failed)
    :param replace_if_false: make the function modify returned value if bool(value)==False
    :param value_if_false: function returns it if if_false==True and bool(value)==False
    :param warn_if_false:
    :param replace_if_conversion_error: make the function modify returned value if conversion fails with ValueError
    :param value_if_conversion_error: the function returns it if replace_if_conversion_error==True and
    conversion fails  with ValueError
    :param warn_if_conversion_error:
    :param replace_if_checker_error: make the function modify returned value if checker raises ValueError
    :param value_if_checker_error: the function returns it if if_checker_error==True and checker raises ValueError
    :param warn_if_checker_error:
    :return: value of type==output_type
    :param info: more info
    """
    if replace_if_false and not bool(value):
        if warn_if_false:
            logging.warning(
                'bool({!r})==False'.format(value) + (' <info: {}>'.format(info) if info else '')
            )
        return value_if_false
    try:
        new_value = output_type(value)
    except ValueError:
        if warn_if_conversion_error:
            logging.warning(
                'Conversion the value of {!r} to type {} failed'.format(value, output_type) + (
                    ' <info: {}>'.format(info) if info else '')
            )
        if replace_if_conversion_error:
            return value_if_conversion_error
        raise
    else:
        try:
            checker(new_value)
        except ValueError:
            if warn_if_checker_error:
                logging.warning(
                    'Checking the value of {!r} with the checker {} failed'.format(new_value, checker) + (
                        ' <info: {}>'.format(info) if info else '')
                )
            if replace_if_checker_error:
                return value_if_checker_error
            raise
        else:
            return new_value


class BaseChecker(abc.ABC):
    @abc.abstractmethod
    def __call__(self, value):
        pass


class MaxLenChecker(BaseChecker):
    def __init__(self, max_len):
        self.max_len = max_len

    def __call__(self, value):
        if len(value) > self.max_len:
            raise ValueError(
                'length of {} is greater than the maximum allowed length of {}'.format(len(value), self.max_len)
            )

    def __repr__(self):
        return '<Checker={}: max_len={}>'.format(self.__class__.__name__, self.max_len)


class RangeChecker(BaseChecker):
    def __init__(self, minimum, maximum):
        self.minimum = minimum
        self.maximum = maximum

    def __call__(self, value):
        if value < self.minimum:
            raise ValueError('{} is less than the minimum allowed of {}'.format(value, self.minimum))
        if value > self.maximum:
            raise ValueError('{} is greater than the maximum allowed of {}'.format(value, self.maximum))

    def __repr__(self):
        return '<Checker={}: minimum={}, maximum={}>'.format(self.__class__.__name__, self.minimum, self.maximum)


class EnumChecker(BaseChecker):
    def __init__(self, valid_values: tuple):
        self.valid_values = valid_values

    def __call__(self, value):
        if value not in self.valid_values:
            raise ValueError(
                'The value of {} is not a valid value. Valid values: {}'.format(value, self.valid_values)
            )

    def __repr__(self):
        return '<Checker={}: valid_values={}>'.format(self.__class__.__name__, self.valid_values)


if __name__ == '__main__':
    a = 14.2
    b = convert_with_check(
        a,
        output_type=int,
        checker=RangeChecker(13, 13),
        replace_if_false=True,
        value_if_false=None,
        warn_if_false=True,
        replace_if_conversion_error=True,
        value_if_conversion_error=None,
        warn_if_conversion_error=True,
        replace_if_checker_error=True,
        value_if_checker_error=None,
        warn_if_checker_error=True,
        info='INFORMATION'
    )
    print(b)
