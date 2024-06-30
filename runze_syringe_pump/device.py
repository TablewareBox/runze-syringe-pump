import asyncio
from asyncio import Event, Future, Lock, Task
from enum import Enum
from dataclasses import dataclass
from typing import Any, Union, Optional, overload

import serial.tools.list_ports
from serial import Serial
from serial.serialutil import SerialException


class RunzeSyringePumpMode(Enum):
    Normal = 0
    AccuratePos = 1
    AccuratePosVel = 2


class RunzeSyringePumpConnectionError(Exception):
    pass


@dataclass(frozen=True, kw_only=True)
class RunzeSyringePumpInfo:
    port: str
    address: str = "1"
    
    volume: float = 25000
    mode: RunzeSyringePumpMode = RunzeSyringePumpMode.Normal

    def create(self):
        return RunzeSyringePump(self.port, self.address, self.volume, self.mode)


class RunzeSyringePump:
    def __init__(self, port: str, address: str = "1", volume: float = 25000, mode: RunzeSyringePumpMode = RunzeSyringePumpMode.Normal):
        self.port = port
        self.address = address
        
        self.volume = volume
        self.mode = mode
        self.total_steps = 6000 if self.mode == RunzeSyringePumpMode.Normal else 48000

        try:
            self._serial = Serial(
                baudrate=9600,
                port=port
            )
        except (OSError, SerialException) as e:
            raise RunzeSyringePumpConnectionError from e

        self._busy = False
        self._closing = False
        self._error_event = Event()
        self._query_future = Future[Any]()
        self._query_lock = Lock()
        self._read_task: Optional[Task[None]] = None
        self._run_future: Optional[Future[Any]] = None
        self._run_lock = Lock()

    async def _read_loop(self):
        try:
            while True:
                self._receive((await asyncio.to_thread(lambda: self._serial.read_until(b"\n")))[3:-3])
        except SerialException as e:
            raise RunzeSyringePumpConnectionError from e
        finally:
            if not self._closing:
                self._error_event.set()

            if self._query_future and not self._query_future.done():
                self._query_future.set_exception(RunzeSyringePumpConnectionError())
            if self._run_future and not self._run_future.done():
                self._run_future.set_exception(RunzeSyringePumpConnectionError())

    @overload
    async def _query(self, command: str, dtype: type[bool]) -> bool:
        pass

    @overload
    async def _query(self, command: str, dtype: type[int]) -> int:
        pass

    @overload
    async def _query(self, command: str, dtype = None) -> str:
        pass

    async def _query(self, command: str, dtype: Optional[type] = None):
        async with self._query_lock:
            if self._closing or self._error_event.is_set():
                raise RunzeSyringePumpConnectionError

            self._query_future = Future[Any]()

            run = 'R' if not command.startswith("?") else ''
            full_command = f"/{self.address}{command}{run}\r\n"
            full_command_data = bytearray(full_command, 'ascii')

            try:
                await asyncio.to_thread(lambda: self._serial.write(full_command_data))
                return self._parse(await asyncio.wait_for(asyncio.shield(self._query_future), timeout=2.0), dtype=dtype)
            except (SerialException, asyncio.TimeoutError) as e:
                self._error_event.set()
                raise RunzeSyringePumpConnectionError from e
            finally:
                self._query_future = None

    def _parse(self, data: bytes, dtype: Optional[type] = None):
        response = data.decode()

        if dtype == bool:
            return response == "1"
        elif dtype == int:
            return int(response)
        else:
            return response

    def _receive(self, data: bytes):
        ascii_string = "".join(chr(byte) for byte in data)
        was_busy = self._busy
        self._busy = ((data[0] & (1 << 5)) < 1) or ascii_string.startswith("@")

        if self._run_future and was_busy and not self._busy:
            self._run_future.set_result(data)
        if self._query_future:
            self._query_future.set_result(data)
        else:
            raise Exception("Dropping data")

    async def _run(self, command: str):
        async with self._run_lock:
            self._run_future = Future[Any]()

            try:
                await self._query(command)
                while True:
                    await asyncio.sleep(0.5)  # Wait for 0.5 seconds before polling again
                    
                    status = await self.query_device_status()
                    if status == '`':
                        break
                await asyncio.shield(self._run_future)
            finally:
                self._run_future = None

    async def initialize(self):
        return await self._run("Z")
    
    # Settings

    async def set_baudrate(self, baudrate):
        if baudrate == 9600:
            return await self._run("U41")
        elif baudrate == 38400:
            return await self._run("U47")
        else:
            raise ValueError("Unsupported baudrate")
    
    async def set_step_mode(self, mode: RunzeSyringePumpMode):
        self.mode = mode
        self.total_steps = 6000 if self.mode == RunzeSyringePumpMode.Normal else 48000
        command = f"N{mode.value}"
        print(command)
        return await self._run(command)

    async def set_speed(self, speed: int):
        return await self._run(f"S{speed}")
    
    # Operations

    async def move_plunger_to(self, volume: float):
        """
        Move to absolute volume (unit: μL)

        Args:
            volume (float): absolute position of the plunger, unit: μL

        Returns:
            None
        """
        pos_step = int(volume / self.volume * self.total_steps)
        return await self._run(f"A{pos_step}")
    
    async def pull_plunger(self, volume: float):
        """
        Pull a fixed volume (unit: μL)

        Args:
            volume (float): absolute position of the plunger, unit: μL

        Returns:
            None
        """
        pos_step = int(volume / self.volume * self.total_steps)
        return await self._run(f"P{pos_step}")
    
    async def push_plunger(self, volume: float):
        """
        Push a fixed volume (unit: μL)

        Args:
            volume (float): absolute position of the plunger, unit: μL

        Returns:
            None
        """
        pos_step = int(volume / self.volume * self.total_steps)
        return await self._run(f"D{pos_step}")

    async def set_valve_position(self, position: Union[int, str]):
        command = f"I{position}" if type(position) == int or ord(position) <= 57 else position.upper() 
        return await self._run(command)

    async def stop_operation(self):
        return await self._run("T")
    
    # Queries

    async def query_device_status(self):
        return await self._query("Q")

    async def report_position(self):
        response = await self._query("?0")
        status, pos_step = response[0], int(response[1:])
        return pos_step / self.total_steps * self.volume

    async def query_start_speed(self):
        return await self._query("?1")

    async def query_max_speed(self):
        return await self._query("?2")

    async def query_cutoff_speed(self):
        return await self._query("?3")

    async def query_plunger_position(self):
        response = await self._query("?4")
        status, pos_step = response[0], int(response[1:])
        return pos_step / self.total_steps * self.volume

    async def query_valve_position(self):
        return await self._query("?6")

    async def query_command_buffer_status(self):
        return await self._query("?10")

    async def query_backlash_position(self):
        return await self._query("?12")

    async def query_aux_input_status_1(self):
        return await self._query("?13")

    async def query_aux_input_status_2(self):
        return await self._query("?14")

    async def query_software_version(self):
        return await self._query("?23")

    async def wait_error(self):
        await self._error_event.wait()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self):
        if self._read_task:
            raise RunzeSyringePumpConnectionError

        self._read_task = asyncio.create_task(self._read_loop())

        try:
            await self.query_device_status()
        except Exception:
            await self.close()
            raise

    async def close(self):
        if self._closing or not self._read_task:
            raise RunzeSyringePumpConnectionError

        self._closing = True
        self._read_task.cancel()

        try:
            await self._read_task
        except asyncio.CancelledError:
            pass
        finally:
            del self._read_task

        self._serial.close()

    @staticmethod
    def list():
        for item in serial.tools.list_ports.comports():
            yield RunzeSyringePumpInfo(port=item.device)
