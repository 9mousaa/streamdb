/**
 * Minimal LZ-string decompressor (decompressFromBase64 only)
 * Based on pieroxy/lz-string (MIT license)
 * Zero dependencies. Only implements decompression path.
 */

const keyStrBase64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=';

function getBaseValue(char: string): number {
  return keyStrBase64.indexOf(char);
}

export function decompressFromBase64(input: string): string | null {
  if (!input || input.length === 0) return '';

  return _decompress(input.length, 32, (index: number) => getBaseValue(input.charAt(index)));
}

function _decompress(length: number, resetValue: number, getNextValue: (index: number) => number): string | null {
  const dictionary: Record<number, string> = {};
  let enlargeIn = 4;
  let dictSize = 4;
  let numBits = 3;
  let entry = '';
  const result: string[] = [];
  let w: string = '';
  let c: string | number = 0;

  let data_val = getNextValue(0);
  let data_position = resetValue;
  let data_index = 1;

  let bits = 0;
  let maxpower = Math.pow(2, 2);
  let power = 1;

  while (power !== maxpower) {
    const resb = data_val & data_position;
    data_position >>= 1;
    if (data_position === 0) {
      data_position = resetValue;
      data_val = getNextValue(data_index++);
    }
    bits |= (resb > 0 ? 1 : 0) * power;
    power <<= 1;
  }

  const next = bits;
  switch (next) {
    case 0:
      bits = 0;
      maxpower = Math.pow(2, 8);
      power = 1;
      while (power !== maxpower) {
        const resb = data_val & data_position;
        data_position >>= 1;
        if (data_position === 0) {
          data_position = resetValue;
          data_val = getNextValue(data_index++);
        }
        bits |= (resb > 0 ? 1 : 0) * power;
        power <<= 1;
      }
      c = String.fromCharCode(bits);
      break;
    case 1:
      bits = 0;
      maxpower = Math.pow(2, 16);
      power = 1;
      while (power !== maxpower) {
        const resb = data_val & data_position;
        data_position >>= 1;
        if (data_position === 0) {
          data_position = resetValue;
          data_val = getNextValue(data_index++);
        }
        bits |= (resb > 0 ? 1 : 0) * power;
        power <<= 1;
      }
      c = String.fromCharCode(bits);
      break;
    case 2:
      return '';
  }

  dictionary[3] = c as string;
  w = c as string;
  result.push(c as string);

  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (data_index > length) return '';

    bits = 0;
    maxpower = Math.pow(2, numBits);
    power = 1;
    while (power !== maxpower) {
      const resb = data_val & data_position;
      data_position >>= 1;
      if (data_position === 0) {
        data_position = resetValue;
        data_val = getNextValue(data_index++);
      }
      bits |= (resb > 0 ? 1 : 0) * power;
      power <<= 1;
    }

    switch (c = bits) {
      case 0:
        bits = 0;
        maxpower = Math.pow(2, 8);
        power = 1;
        while (power !== maxpower) {
          const resb = data_val & data_position;
          data_position >>= 1;
          if (data_position === 0) {
            data_position = resetValue;
            data_val = getNextValue(data_index++);
          }
          bits |= (resb > 0 ? 1 : 0) * power;
          power <<= 1;
        }
        dictionary[dictSize++] = String.fromCharCode(bits);
        c = dictSize - 1;
        enlargeIn--;
        break;
      case 1:
        bits = 0;
        maxpower = Math.pow(2, 16);
        power = 1;
        while (power !== maxpower) {
          const resb = data_val & data_position;
          data_position >>= 1;
          if (data_position === 0) {
            data_position = resetValue;
            data_val = getNextValue(data_index++);
          }
          bits |= (resb > 0 ? 1 : 0) * power;
          power <<= 1;
        }
        dictionary[dictSize++] = String.fromCharCode(bits);
        c = dictSize - 1;
        enlargeIn--;
        break;
      case 2:
        return result.join('');
    }

    if (enlargeIn === 0) {
      enlargeIn = Math.pow(2, numBits);
      numBits++;
    }

    if (dictionary[c as number]) {
      entry = dictionary[c as number];
    } else {
      if (c === dictSize) {
        entry = w + w.charAt(0);
      } else {
        return null;
      }
    }

    result.push(entry);
    dictionary[dictSize++] = w + entry.charAt(0);
    enlargeIn--;
    w = entry;

    if (enlargeIn === 0) {
      enlargeIn = Math.pow(2, numBits);
      numBits++;
    }
  }
}
