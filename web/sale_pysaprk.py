def is_valid_number(number):
  """
  Checks if a given number is valid in the hacker's game (rotates 180 degrees to form a new valid digit).

  Args:
      number: The integer to check.

  Returns:
      True if the number is valid, False otherwise.
  """
  valid_digits = {0, 1, 6, 8, 9}  # Numbers that are valid after 180-degree rotation
  rotated_number = 0

  # Reverse the digits of the number
  while number > 0:
    digit = number % 10
    rotated_number = rotated_number * 10 + digit
    number //= 10

  # Check if the rotated number is also valid
  return rotated_number in valid_digits

def count_valid_numbers(n):
  """
  Counts the number of valid numbers between 1 and n (inclusive) in the hacker's game.

  Args:
      n: The upper limit of the range.

  Returns:
      The number of valid numbers.
  """
  count = 0
  for num in range(1, n + 1):
    if is_valid_number(num):
      count += 1

  return count

# Example usage
n = int(input())
valid_count = count_valid_numbers(n)
print(f"Number of valid numbers between 1 and {n}: {valid_count}")
