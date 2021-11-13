"""Functions for CP2K-specific input/output."""

__all__ = ['add_energy_extxyz_comment', 'read_frames_extxyz']

from itertools import repeat
from shlex import split
from ..constants import angstrom, eV
import numpy as np
from .utilities import Frame, merge_frames, read_frames


def add_energy_extxyz_comment(frames,pos_unit, e_unit):
    """Parse CP2K energy and inject it into frames.

    For each frame in `frames`, try to extract a CP2K-formatted potential energy
    from the comment string and inject it back into the frame. Energy from CP2K is
    in Hartree, so no conversion is needed.
    """

    for frame in frames:

        if frame.energy is not None:
            raise ValueError('Energy already present.')
        if frame.cell is not None:
            raise ValueError('cell already present.')

        try:
            for pair in split(frame.comment):
                items = pair.split('=')
                if items[0].strip() == 'energy':
                    frame.energy = float(items[1])*e_unit
                if items[0].strip() == 'Lattice':
                    frame.cell == np.reshape(np.array([float(x)*pos_unit for x in items[1].split() ]),(3,3))
        except (IndexError, ValueError):
            raise ValueError('No energy or cell found in comment line.')

        yield frame


def read_frames_extxyz(fn, pos_unit=angstrom, force_unit=eV/angstrom,e_unit=eV):
    """Read data specifically produced by CP2K.

    Arguments:
        fn_positions: position trajectory file name, XYZ format
        cell: a constant cell to use in all frames, optional
        fn_forces: forces file name, XYZ format, optional
        read_energy: whether to read energies from comments in `fn_positions`

    Returns:
        a `Frame` object
    """

    # positions from XYZ, energies from comment if requested
    # we expect units of angstrom for positions from CP2K
    frames_pos = read_frames(fn, name_data='posforces', pos_unit=pos_unit, force_unit=force_unit,fformat='extxyz')
    frames_pos = add_energy_extxyz_comment(frames_pos,pos_unit=pos_unit, e_unit=e_unit)
    frames = [frames_pos]

    # iterate over merged frames
    yield from merge_frames(*frames)
