import asyncio
import http.cookies
import json
import re
import subprocess
from logging import INFO
from time import time
from urllib.parse import quote, urljoin

import m3u8
from aiohttp import ClientSession, TCPConnector
from multidict import CIMultiDictProxy

# 导入项目常量/配置/工具函数
import utils.constants as constants
from utils.config import config
from utils.tools import get_resolution_value, get_logger
from utils.types import TestResult, ChannelTestResult, TestResultCacheData

# ===================== 全局配置初始化（高分辨率+秒播核心） =====================
# 修复Cookie解析兼容问题
http.cookies._is_legal_key = lambda _: True
# 测速结果缓存（避免重复检测，提升秒播效率）
cache: TestResultCacheData = {}
# 从配置读取核心参数（高分辨率+秒播适配）
speed_test_timeout = config.speed_test_timeout          # 测速超时（高分辨率需稍长，默认6秒）
speed_test_filter_host = config.speed_test_filter_host  # 是否按Host过滤缓存（关闭兼容全网通）
open_filter_resolution = config.open_filter_resolution  # 启用高分辨率过滤（核心开关）
min_resolution_value = config.min_resolution_value      # 最低分辨率值（1920x1080=2073600）
max_resolution_value = config.max_resolution_value      # 最高分辨率值（4K=16588800）
open_supply = config.open_supply                        # 高分辨率接口不足时补充（避免频道缺失）
open_filter_speed = config.open_filter_speed            # 启用速率过滤（高分辨率需1.2M/s以上）
min_speed_value = config.min_speed                      # 最低速率阈值（1.2M/s，保证高分辨率不卡顿）
# M3U8格式头兼容（高分辨率流常见格式）
m3u8_headers = ['application/x-mpegurl', 'application/vnd.apple.mpegurl', 'audio/mpegurl', 'audio/x-mpegurl']
# IPv6默认配置（全网通兼容，高分辨率默认1080P）
default_ipv6_delay = 0.1
default_ipv6_resolution = "1920x1080"
default_ipv6_result = {
    'speed': float("inf"),
    'delay': default_ipv6_delay,
    'resolution': default_ipv6_resolution
}
# 初始化测速日志（高分辨率检测日志单独记录）
logger = get_logger(constants.speed_test_log_path, level=INFO, init=True)


async def get_speed_with_download(url: str, headers: dict = None, session: ClientSession = None,
                                  timeout: int = speed_test_timeout) -> dict[str, float | None]:
    """
    【高分辨率优化】下载测速（兼顾带宽和延迟，保证4K/1080P不卡顿）
    :param url: 待检测URL
    :param headers: 请求头
    :param session: 复用aiohttp会话（减少连接耗时）
    :param timeout: 超时时间（高分辨率默认6秒）
    :return: 测速结果（速度/延迟/大小/耗时）
    """
    start_time = time()
    delay = -1  # 初始化延迟（毫秒）
    total_size = 0  # 下载总大小（字节）
    # 复用会话（减少高分辨率流的连接耗时）
    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False

    try:
        # 流式下载（避免4K流一次性加载卡顿）
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status != 200:
                raise Exception("无效响应码，跳过该接口")
            # 计算连接延迟（高分辨率秒播核心指标）
            delay = int(round((time() - start_time) * 1000))
            # 分块读取（兼容4K大码率流）
            async for chunk in response.content.iter_any():
                if chunk:
                    total_size += len(chunk)
    except Exception as e:
        logger.warning(f"下载测速失败 {url}: {e}")
        pass
    finally:
        total_time = time() - start_time
        # 关闭临时会话
        if created_session:
            await session.close()
        # 计算速度（MB/s），避免除以0
        speed = total_size / total_time / 1024 / 1024 if total_time > 0 else 0
        return {
            'speed': speed,          # 下载速度（MB/s）
            'delay': delay,          # 连接延迟（毫秒）
            'size': total_size,      # 下载大小（字节）
            'time': total_time,      # 总耗时（秒）
        }


async def get_headers(url: str, headers: dict = None, session: ClientSession = None, timeout: int = 5) -> CIMultiDictProxy[str] | dict[any, any]:
    """
    【全网通优化】获取URL响应头（快速判断M3U8格式，减少无效检测）
    :return: 响应头字典
    """
    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False

    res_headers = {}
    try:
        # HEAD请求（仅获取头，不下载内容，提升检测速度）
        async with session.head(url, headers=headers, timeout=timeout) as response:
            res_headers = response.headers
    except Exception as e:
        logger.warning(f"获取响应头失败 {url}: {e}")
        pass
    finally:
        if created_session:
            await session.close()
        return res_headers


async def get_url_content(url: str, headers: dict = None, session: ClientSession = None,
                          timeout: int = speed_test_timeout) -> str:
    """
    【高分辨率优化】获取URL内容（解析M3U8中的4K/1080P流）
    :return: URL文本内容
    """
    if session is None:
        session = ClientSession(connector=TCPConnector(ssl=False), trust_env=True)
        created_session = True
    else:
        created_session = False

    content = ""
    try:
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status == 200:
                content = await response.text()
            else:
                raise Exception(f"响应码异常: {response.status}")
    except Exception as e:
        logger.warning(f"获取URL内容失败 {url}: {e}")
        pass
    finally:
        if created_session:
            await session.close()
        return content


def check_m3u8_valid(headers: CIMultiDictProxy[str] | dict[any, any]) -> bool:
    """
    【格式校验】判断是否为有效M3U8流（高分辨率流核心格式）
    :return: 是否有效
    """
    content_type = headers.get('Content-Type', '').lower()
    if not content_type:
        return False
    # 兼容所有M3U8变种格式
    return any(item in content_type for item in m3u8_headers)


async def get_result(url: str, headers: dict = None, resolution: str = None,
                     filter_resolution: bool = config.open_filter_resolution,
                     timeout: int = speed_test_timeout) -> dict[str, float | None]:
    """
    【核心逻辑】获取高分辨率流的测速结果（优先检测4K/1080P，保证秒播）
    :return: 速度/延迟/分辨率结果
    """
    # 初始化结果（默认低分辨率，后续覆盖）
    info = {'speed': 0, 'delay': -1, 'resolution': resolution}
    location = None  # 重定向地址

    try:
        # URL编码处理（兼容特殊字符，全网通适配）
        url = quote(url, safe=':/?$&=@[]%').partition('$')[0]
        async with ClientSession(connector=TCPConnector(ssl=False), trust_env=True) as session:
            # 第一步：获取响应头，判断是否为M3U8
            res_headers = await get_headers(url, headers, session)
            location = res_headers.get('Location')

            # 处理重定向（全网通适配，避免跨运营商重定向卡顿）
            if location:
                info.update(await get_result(location, headers, resolution, filter_resolution, timeout))
            else:
                # 第二步：解析M3U8内容，优先选最高码率流（4K/1080P）
                url_content = await get_url_content(url, headers, session, timeout)
                if url_content:
                    m3u8_obj = m3u8.loads(url_content)
                    playlists = m3u8_obj.playlists  # 多码率流列表
                    segments = m3u8_obj.segments    # 单码率流片段

                    # 多码率流：优先选最高带宽（4K>1080P）
                    if playlists:
                        # 按带宽排序，选最高码率（高分辨率核心）
                        best_playlist = max(m3u8_obj.playlists, key=lambda p: p.stream_info.bandwidth)
                        playlist_url = urljoin(url, best_playlist.uri)
                        # 获取最高码率流的内容
                        playlist_content = await get_url_content(playlist_url, headers, session, timeout)
                        if playlist_content:
                            media_playlist = m3u8.loads(playlist_content)
                            segment_urls = [urljoin(playlist_url, segment.uri) for segment in media_playlist.segments]
                    # 单码率流：直接取片段
                    else:
                        segment_urls = [urljoin(url, segment.uri) for segment in segments]

                    # 无片段URL，判定为无效流
                    if not segment_urls:
                        raise Exception("未找到视频片段URL，跳过")
                else:
                    # 非M3U8流，直接下载测速（兼容RTMP等格式）
                    res_info = await get_speed_with_download(url, headers, session, timeout)
                    info.update({'speed': res_info['speed'], 'delay': res_info['delay']})
                    raise Exception("非M3U8格式，使用下载测速")

                # 第三步：检测前5个片段（平衡速度和准确性，避免4K流检测卡顿）
                start_time = time()
                tasks = [get_speed_with_download(ts_url, headers, session, timeout) for ts_url in segment_urls[:5]]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 计算平均速度（高分辨率流需累加片段大小）
                total_size = sum(result['size'] for result in results if isinstance(result, dict))
                total_time = sum(result['time'] for result in results if isinstance(result, dict))
                info['speed'] = total_size / total_time / 1024 / 1024 if total_time > 0 else 0
                # 计算整体延迟（秒播核心指标）
                info['delay'] = int(round((time() - start_time) * 1000))
    except Exception as e:
        logger.warning(f"获取结果失败 {url}: {e}")
        pass
    finally:
        # 自动检测分辨率（未指定时，高分辨率过滤核心）
        if not resolution and filter_resolution and not location and info['delay'] != -1:
            info['resolution'] = await get_resolution_ffprobe(url, headers, timeout)
        return info


async def get_delay_requests(url, timeout=speed_test_timeout, proxy=None):
    """
    【秒播优化】快速检测URL延迟（仅检测连接，不下载内容，提升效率）
    :return: 延迟（毫秒），-1表示失败
    """
    async with ClientSession(connector=TCPConnector(ssl=False), trust_env=True) as session:
        start = time()
        end = None
        try:
            async with session.get(url, timeout=timeout, proxy=proxy) as response:
                if response.status == 404:
                    return -1
                # 仅读取少量内容，判断是否可达（秒播核心）
                content = await response.read()
                if content:
                    end = time()
                else:
                    return -1
        except Exception as e:
            logger.warning(f"延迟检测失败 {url}: {e}")
            return -1
        # 计算延迟（毫秒）
        return int(round((end - start) * 1000)) if end else -1


def check_ffmpeg_installed_status():
    """
    【依赖检测】检查FFmpeg是否安装（高分辨率检测必需）
    :return: 是否安装
    """
    status = False
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        status = result.returncode == 0
    except FileNotFoundError:
        logger.error("FFmpeg未安装，无法检测高分辨率！")
        status = False
    except Exception as e:
        logger.error(f"FFmpeg检测异常: {e}")
    finally:
        return status


async def ffmpeg_url(url, timeout=speed_test_timeout):
    """
    【高分辨率优化】调用FFmpeg检测URL信息（兼容4K/1080P解析）
    :return: FFmpeg输出结果
    """
    # FFmpeg参数（仅检测，不转码，减少耗时）
    args = ["ffmpeg", "-t", str(timeout), "-stats", "-i", url, "-f", "null", "-"]
    proc = None
    res = None
    try:
        # 创建异步子进程（避免阻塞，提升秒播效率）
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        # 超时保护（高分辨率检测需稍长）
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout + 2)
        if out:
            res = out.decode("utf-8")
        if err:
            res = err.decode("utf-8")
    except asyncio.TimeoutError:
        logger.warning(f"FFmpeg检测超时 {url}")
        if proc:
            proc.kill()
    except Exception as e:
        logger.error(f"FFmpeg调用失败 {url}: {e}")
        if proc:
            proc.kill()
    finally:
        if proc:
            await proc.wait()
        return res


async def get_resolution_ffprobe(url: str, headers: dict = None, timeout: int = speed_test_timeout) -> str | None:
    """
    【核心】通过ffprobe精准检测分辨率（4K/1080P/720P）
    :return: 分辨率（如1920x1080），None表示失败
    """
    resolution = None
    proc = None
    # 检查FFmpeg是否安装（前置条件）
    if not check_ffmpeg_installed_status():
        return None

    try:
        # ffprobe参数（仅获取视频流分辨率，精准高效）
        probe_args = [
            'ffprobe',
            '-v', 'error',                # 仅输出错误，减少日志
            '-headers', ''.join(f'{k}: {v}\r\n' for k, v in headers.items()) if headers else '',  # 自定义请求头
            '-select_streams', 'v:0',     # 仅选第一个视频流
            '-show_entries', 'stream=width,height',  # 仅获取宽高
            "-of", 'json',                # JSON格式输出，便于解析
            url
        ]
        # 异步执行ffprobe
        proc = await asyncio.create_subprocess_exec(*probe_args, stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)
        # 超时保护
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        # 解析分辨率（兼容4K/1080P）
        video_stream = json.loads(out.decode('utf-8'))["streams"][0]
        resolution = f"{video_stream['width']}x{video_stream['height']}"
    except Exception as e:
        logger.warning(f"分辨率检测失败 {url}: {e}")
        if proc:
            proc.kill()
    finally:
        if proc:
            await proc.wait()
        return resolution


def get_video_info(video_info):
    """
    【辅助函数】解析FFmpeg输出的视频信息（帧率/分辨率）
    :return: 帧率/分辨率
    """
    frame_size = -1
    resolution = None
    if video_info is not None:
        info_data = video_info.replace(" ", "")
        # 提取帧率（判断流是否有效）
        matches = re.findall(r"frame=(\d+)", info_data)
        if matches:
            frame_size = int(matches[-1])
        # 提取分辨率（兼容4K/1080P/720P）
        match = re.search(r"(\d{3,4}x\d{3,4})", info_data)
        if match:
            resolution = match.group(0)
    return frame_size, resolution


async def check_stream_delay(url_info):
    """
    【秒播优化】检测流延迟并提取分辨率（高分辨率+秒播双核心）
    :return: (url_info, 帧率) 或 -1
    """
    try:
        url = url_info["url"]
        # 调用FFmpeg检测
        video_info = await ffmpeg_url(url)
        if video_info is None:
            return -1
        # 解析帧率和分辨率
        frame, resolution = get_video_info(video_info)
        if frame is None or frame == -1:
            return -1
        # 补充分辨率信息（高分辨率过滤核心）
        url_info["resolution"] = resolution
        return url_info, frame
    except Exception as e:
        logger.error(f"流延迟检测失败 {url_info['url']}: {e}")
        return -1


def get_avg_result(result) -> TestResult:
    """
    【缓存优化】计算缓存结果的平均值（避免重复检测，提升秒播）
    :return: 平均速度/延迟/最高分辨率
    """
    return {
        'speed': sum(item['speed'] or 0 for item in result) / len(result),
        'delay': max(int(sum(item['delay'] or -1 for item in result) / len(result)), -1),
        'resolution': max((item['resolution'] for item in result), key=get_resolution_value)  # 优先最高分辨率
    }


def get_speed_result(key: str) -> TestResult:
    """
    【缓存优化】从缓存获取测速结果（提升秒播效率）
    :return: 缓存结果
    """
    if key in cache:
        return get_avg_result(cache[key])
    else:
        return {'speed': 0, 'delay': -1, 'resolution': 0}


async def get_speed(data, headers=None, ipv6_proxy=None, filter_resolution=open_filter_resolution,
                    timeout=speed_test_timeout, callback=None) -> TestResult:
    """
    【入口函数】获取URL的测速结果（高分辨率+秒播+全网通核心）
    :return: 最终测速结果
    """
    url = data['url']
    resolution = data['resolution']
    # 初始化结果
    result: TestResult = {'speed': 0, 'delay': -1, 'resolution': resolution}

    try:
        # 缓存键（按Host过滤，减少重复检测，全网通适配）
        cache_key = data['host'] if speed_test_filter_host else url
        # 优先从缓存获取（秒播核心）
        if cache_key and cache_key in cache:
            result = get_avg_result(cache[cache_key])
        else:
            # IPv6处理（全网通兼容，默认高分辨率）
            if data['ipv_type'] == "ipv6" and ipv6_proxy:
                result.update(default_ipv6_result)
            # RTMP流处理（高分辨率兼容，直接检测分辨率）
            elif constants.rt_url_pattern.match(url) is not None:
                start_time = time()
                # 自动检测分辨率（高分辨率过滤）
                if not result['resolution'] and filter_resolution:
                    result['resolution'] = await get_resolution_ffprobe(url, headers, timeout)
                result['delay'] = int(round((time() - start_time) * 1000))
                # RTMP流标记为无限速度（优先保留）
                if result['resolution'] is not None:
                    result['speed'] = float("inf")
            # 普通流：调用核心检测逻辑
            else:
                result.update(await get_result(url, headers, resolution, filter_resolution, timeout))
            # 加入缓存（提升后续检测速度）
            if cache_key:
                cache.setdefault(cache_key, []).append(result)
    finally:
        # 回调函数（进度更新）
        if callback:
            callback()
        # 日志记录（高分辨率+秒播关键信息）
        logger.info(
            f"频道名: {data.get('name')}, URL: {data.get('url')}, 来源: {data.get('origin')}, "
            f"IP类型: {data.get('ipv_type')}, 地区: {data.get('location')}, 运营商: {data.get('isp')}, "
            f"日期: {data['date']}, 延迟: {result.get('delay') or -1} ms, 速度: {result.get('speed') or 0:.2f} M/s, "
            f"分辨率: {result.get('resolution') or '未知'}"
        )
        return result


def get_sort_result(
        results,
        supply=open_supply,
        filter_speed=open_filter_speed,
        min_speed=min_speed_value,
        filter_resolution=open_filter_resolution,
        min_resolution=min_resolution_value,
        max_resolution=max_resolution_value,
        ipv6_support=True
) -> list[ChannelTestResult]:
    """
    【最终排序】筛选并排序结果（高分辨率优先+秒播无卡顿核心）
    :return: 排序后的高分辨率秒播频道列表
    """
    total_result = []
    for result in results:
        # 过滤IPv6（如需）
        if not ipv6_support and result["ipv_type"] == "ipv6":
            result.update(default_ipv6_result)
        # 提取核心参数
        result_speed, result_delay, resolution = (
            result.get("speed") or 0,
            result.get("delay"),
            result.get("resolution")
        )
        # 过滤无效延迟（-1表示不可达）
        if result_delay == -1:
            continue
        # 非补充模式：严格过滤（高分辨率+速率）
        if not supply:
            # 速率过滤（高分辨率需1.2M/s以上，避免卡顿）
            if filter_speed and result_speed < min_speed:
                continue
            # 分辨率过滤（仅保留1080P/4K）
            if filter_resolution and resolution:
                resolution_value = get_resolution_value(resolution)
                if resolution_value < min_resolution or resolution_value > max_resolution:
                    continue
        # 加入有效结果
        total_result.append(result)

    # 排序：速度从高到低（秒播优先），同时保留高分辨率
    total_result.sort(key=lambda item: (
        get_resolution_value(item.get('resolution') or '0x0'),  # 第一优先级：分辨率（高→低）
        item.get("speed") or 0                                 # 第二优先级：速度（高→低）
    ), reverse=True)
    return total_result
