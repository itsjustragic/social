import os  
import random
import requests
import asyncio
import logging
import re
import string
import aiohttp
import json
import shutil
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    Message,
    InputFile,
    InputMediaPhoto,
    InputMediaVideo,
    FSInputFile,
    BotCommand,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command

# ─── Configuration Files ───────────────────────────────────────────────────────
WHITELIST_FILE = "wit.json"            # JSON file containing whitelisted user IDs
AUTOPOST_CONFIG_FILE = "autoposttt.json"
HD_URLS_FILE = "hd_urlss.json"
PROCESSED_VIDEOS_GROUP_FILE = "processed_videos_group.json"

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.DEBUG)

# ─── Load/Save Whitelist ───────────────────────────────────────────────────────
def load_whitelist() -> set[int]:
    if os.path.exists(WHITELIST_FILE):
        with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # Expecting a dict with key "whitelist" → list of ints
                return set(int(uid) for uid in data.get("whitelist", []))
            except Exception as e:
                logging.error("Failed to parse %s: %s", WHITELIST_FILE, e)
                return set()
    else:
        # Initialize the file if it does not exist
        initial = {"whitelist": []}
        with open(WHITELIST_FILE, "w", encoding="utf-8") as f:
            json.dump(initial, f, indent=2)
        return set()

def save_whitelist(whitelist: set[int]) -> None:
    data = {"whitelist": sorted(list(whitelist))}
    with open(WHITELIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logging.debug("Saved whitelist: %s", data)

# Initialize the in-memory whitelist set
WHITELISTED_USER_IDS: set[int] = load_whitelist()

# ─── Global In-Memory Locks & State ────────────────────────────────────────────
TIMEOUT = 15
tiktok_session = requests.Session()

# ─── Processed Video IDs Per Group Storage ─────────────────────────────────────
def load_processed_videos_group():
    if os.path.exists(PROCESSED_VIDEOS_GROUP_FILE):
        with open(PROCESSED_VIDEOS_GROUP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {group: set(videos) for group, videos in data.items()}
    return {}

def save_processed_videos_group(data):
    serializable = {group: list(videos) for group, videos in data.items()}
    with open(PROCESSED_VIDEOS_GROUP_FILE, "w", encoding="utf-8") as f:
        json.dump(serializable, f, indent=2, ensure_ascii=False)

PROCESSED_VIDEOS_GROUP: Dict[str, set] = load_processed_videos_group()

def should_process_video_group(video_id: str, group_id: str) -> bool:
    return video_id not in PROCESSED_VIDEOS_GROUP.get(group_id, set())

def mark_video_processed_group(video_id: str, group_id: str) -> None:
    if group_id not in PROCESSED_VIDEOS_GROUP:
        PROCESSED_VIDEOS_GROUP[group_id] = set()
    PROCESSED_VIDEOS_GROUP[group_id].add(video_id)
    save_processed_videos_group(PROCESSED_VIDEOS_GROUP)
    logging.debug("Marked video %s as processed for group %s", video_id, group_id)

def unmark_video_processed_group(video_id: str, group_id: str) -> None:
    if group_id in PROCESSED_VIDEOS_GROUP and video_id in PROCESSED_VIDEOS_GROUP[group_id]:
        PROCESSED_VIDEOS_GROUP[group_id].remove(video_id)
        save_processed_videos_group(PROCESSED_VIDEOS_GROUP)
        logging.debug("Unmarked video %s for group %s", video_id, group_id)

# ─── HD URLs and Callback Storage ──────────────────────────────────────────────
def load_hd_urls():
    if os.path.exists(HD_URLS_FILE):
        with open(HD_URLS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_hd_urls(data):
    with open(HD_URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

HD_URLS: Dict[str, str] = load_hd_urls()
AUDIO_URLS: Dict[str, str] = {}

HD_CALLBACKS = {}
AUDIO_CALLBACKS = {}

def generate_callback_key(prefix: str) -> str:
    random_part = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{prefix}_{random_part}"

# ─── Autopost Config ─────────────────────────────────────────────────────────
def load_autopost_config() -> dict:
    if os.path.exists(AUTOPOST_CONFIG_FILE):
        with open(AUTOPOST_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_autopost_config(config: dict) -> None:
    # no need to mkdir since it's just a filename in cwd
    with open(AUTOPOST_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ─── TikTok User Data & Video Functions ────────────────────────────────────────
def get_user_data(username):
    user_data_file = os.path.join("Data", username, "UserData.json")
    if os.path.exists(user_data_file):
        with open(user_data_file, "r", encoding="utf-8") as file:
            data = json.load(file)
        logging.debug("Loaded user data for %s from file.", username)
        return data["data"][0]["sec_uid"], data["data"][0]["uid"], data
    else:
        logging.debug("User data file not found for %s. Fetching using page method...", username)
        page = get_page(username)
        dirpath = os.path.join("Data", username)
        os.makedirs(dirpath, exist_ok=True)
        if page is not None and page.status_code == 200:
            sec_uid, uid = get_user_info(username, page.text)
            if sec_uid and uid:
                data_to_write = {"data": [{"sec_uid": sec_uid, "uid": uid}]}
                with open(user_data_file, "w", encoding="utf-8") as file:
                    json.dump(data_to_write, file, ensure_ascii=False, indent=2)
                logging.debug("Fetched and saved user data for %s.", username)
                return sec_uid, uid, data_to_write
            else:
                logging.error("Failed to fetch user info for %s", username)
                return None, None, None
        else:
            logging.error("Failed to load page for %s", username)
            return None, None, None

def get_page(user_id):
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html"}
    try:
        logging.debug("Requesting page for user: %s", user_id)
        return tiktok_session.get(f"https://www.tiktok.com/@{user_id}/", headers=headers, timeout=TIMEOUT)
    except requests.exceptions.Timeout:
        logging.error("Timeout while fetching page for user: %s", user_id)
        return None
    except Exception as e:
        logging.error("Error requesting user page: %s", e)
        return None

def get_user_info(username, page_text):
    try:
        user_id = re.search(r'{"userInfo":{"user":{"id":"(\d+)"', page_text).group(1)
        sec_id = re.search(r'secUid":"(.*?)"', page_text).group(1)
        logging.debug("Extracted user info for %s: uid=%s, sec_uid=%s", username, user_id, sec_id)
        return sec_id, user_id
    except Exception as e:
        logging.error("Could not find user info for %s: %s", username, e)
        return None, None

def get_video_ids(username, uid, sec_uid):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        logging.debug("Fetching video list for user: %s", username)
        response = tiktok_session.get(
            f"https://www.tiktok.com/api/creator/item_list/?aid=1988&type=1&count=15&cursor=0&secUid={sec_uid}&verifyFp=verify_",
            headers=headers,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        video_list = data.get("itemList", [])
        logging.debug("Fetched %d videos for user %s", len(video_list), username)
        return video_list
    except Exception as e:
        logging.error("Error fetching video IDs for %s: %s", username, e)
        return []

async def download_video(video_id, username, uid, message=None, group_id: str = None):
    """
    Downloads a video or images. Duplicate prevention is based on video_id.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_download_dir = os.path.join(script_dir, "Downloads")
    user_folder = f"{username}_{uid}"
    save_dir = os.path.join(base_download_dir, user_folder)

    if not os.path.exists(base_download_dir):
        try:
            os.makedirs(base_download_dir, exist_ok=True)
        except Exception as e:
            logging.error("Failed to create 'Downloads' folder: %s", e)
            return None

    try:
        os.makedirs(save_dir, exist_ok=True)
    except Exception as e:
        logging.error("Failed to create user subfolder %s: %s", save_dir, e)
        return None

    video_file_path = os.path.join(save_dir, f"{video_id}.mp4")

    if os.path.exists(video_file_path):
        logging.debug("Video %s already exists for user %s.", video_id, username)
        return video_file_path

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        logging.debug("Requesting download URL for video %s", video_id)
        response = tiktok_session.get(f"https://www.tikwm.com/api/?url={video_id}&hd=1", headers=headers, timeout=TIMEOUT)
        video_info = response.json()
        data = video_info.get("data", {})

        if "images" in data and data["images"]:
            image_paths = []
            for i, image_url in enumerate(data["images"]):
                image_file_path = os.path.join(save_dir, f"{video_id}_{i+1}.jpg")
                image_resp = requests.get(image_url, headers=headers, stream=True)
                if image_resp.status_code == 200:
                    try:
                        with open(image_file_path, "wb") as file:
                            for chunk in image_resp.iter_content(chunk_size=1024):
                                file.write(chunk)
                        logging.debug("Downloaded image %d for video %s to %s", i + 1, video_id, image_file_path)
                    except Exception as e:
                        logging.error("Failed to write image file %s: %s", image_file_path, e)
                        continue
                    image_paths.append(image_file_path)
                else:
                    logging.error(
                        "Failed to download image %d for video %s: HTTP %s",
                        i + 1,
                        video_id,
                        image_resp.status_code,
                    )
            return image_paths if len(image_paths) > 1 else image_paths[0]

        elif "play" in data and data["play"]:
            video_url = data["play"]
            if "sf16-ies-music-va.tiktokcdn.com" in video_url:
                logging.debug(
                    "Video %s uses a music domain URL (%s). Skipping download.",
                    video_id,
                    video_url,
                )
                return None

            video_resp = requests.get(video_url, headers=headers, stream=True, timeout=TIMEOUT)
            if video_resp.status_code == 200:
                try:
                    with open(video_file_path, "wb") as file:
                        for chunk in video_resp.iter_content(chunk_size=1024):
                            file.write(chunk)
                    logging.debug("Downloaded video %s for user %s to %s", video_id, username, video_file_path)
                    return video_file_path
                except Exception as e:
                    logging.error("Failed to write video file %s: %s", video_file_path, e)
            else:
                logging.error("Failed to download video %s: HTTP %s", video_id, video_resp.status_code)
        else:
            logging.error("No downloadable media found for video %s", video_id)
    except Exception as e:
        logging.error("Error downloading video %s for user %s: %s", video_id, username, e)
    return None

class TikTokDownloader:
    def __init__(self, save_path: str = "tiktok_videos"):
        self.save_path = save_path
        self.create_save_directory()

    def create_save_directory(self) -> None:
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
        logging.debug("Save directory ensured at %s", self.save_path)

    @staticmethod
    def validate_url(url: str) -> bool:
        tiktok_pattern = r"https?://((?:vm|vt|www)\.)?tiktok\.com/.*"
        valid = bool(re.match(tiktok_pattern, url))
        logging.debug("URL validation for %s: %s", url, valid)
        return valid

    @staticmethod
    def get_username_video_url(username: str) -> str:
        return f"https://www.tiktok.com/@{username}"

    def get_filename(self, video_url: str, custom_name: Optional[str] = None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = (
            f"{custom_name}_{timestamp}.mp4"
            if custom_name
            else f"TTK_TikTok{timestamp}.mp4"
        )
        logging.debug("Generated filename: %s", filename)
        return filename

    async def download_video(
        self, video_url: str, custom_name: Optional[str] = None, group_id: str = None
    ) -> Optional[str]:
        if not self.validate_url(video_url):
            return None

        username = video_url.split("@")[1] if "@" in video_url else video_url.split("/t/")[1]
        sec_uid, uid, _ = get_user_data(username)
        if sec_uid and uid:
            video_ids = get_video_ids(username, uid, sec_uid)
            if video_ids:
                for video in video_ids:
                    vid = video["id"]
                    if not should_process_video_group(vid, group_id):
                        logging.debug("Skipping duplicate video %s for group %s", vid, group_id)
                        continue
                    mark_video_processed_group(vid, group_id)
                    file_path = await download_video(vid, username, uid, group_id=group_id)
                    if file_path:
                        return file_path
        return None

    async def download_recent_videos(
        self, username: str, num_videos: int = 5, group_id: str = None
    ) -> list:
        sec_uid, uid, _ = get_user_data(username)
        downloaded_files = []
        if sec_uid and uid:
            video_ids = get_video_ids(username, uid, sec_uid)
            current_time = int(datetime.now().timestamp())
            for video in video_ids:
                vid = video["id"]
                try:
                    video_time = int(video.get("createTime", "0"))
                except Exception as e:
                    logging.error("Error converting createTime for video %s: %s", vid, e)
                    continue
                if current_time - video_time <= 86400:
                    if not should_process_video_group(vid, group_id):
                        logging.debug("Skipping duplicate video %s for group %s", vid, group_id)
                        continue
                    mark_video_processed_group(vid, group_id)
                    file_path = await download_video(vid, username, uid, group_id=group_id)
                    if file_path:
                        downloaded_files.append(file_path)
                    if len(downloaded_files) >= num_videos:
                        break
            logging.debug("Downloaded %d recent videos for user %s", len(downloaded_files), username)
            return downloaded_files
        else:
            logging.error("Could not retrieve user data for %s", username)
        return []

# ─── Safe Download Wrapper ─────────────────────────────────────────────────────
PROCESSING_VIDEOS = set()

async def safe_download_video(video_id, username, uid, group_id: str):
    if video_id in PROCESSING_VIDEOS:
        logging.debug("Video %s is already being processed", video_id)
        return None
    PROCESSING_VIDEOS.add(video_id)
    try:
        file_path = await download_video(video_id, username, uid, group_id=group_id)
        return file_path
    finally:
        PROCESSING_VIDEOS.remove(video_id)

# New global storage for Video URLs callbacks
VIDEO_URL_CALLBACKS = {}

# ─── Autopost Loop (using persistent video ID checking) ────────────────────────
async def check_updates_for_username(username: str, group_id: str, config: dict):
    sec_uid, uid, _ = get_user_data(username)
    if not sec_uid or not uid:
        logging.debug("Could not get user data for %s", username)
        return

    video_list = get_video_ids(username, uid, sec_uid)
    if not video_list:
        logging.debug("No videos found for username %s", username)
        return

    video_list.sort(key=lambda v: int(v.get("createTime", "0")))

    # Determine timestamp of last processed video (per config)
    last_processed_time = 0
    last_downloaded_mapping = config.get(group_id, {}).get("last_downloaded", {})
    if username in last_downloaded_mapping:
        last_downloaded_id = last_downloaded_mapping[username]
        for video in video_list:
            if video.get("id") == last_downloaded_id:
                try:
                    last_processed_time = int(video.get("createTime", "0"))
                except Exception as e:
                    logging.error(
                        "Error converting last downloaded createTime for video %s: %s",
                        video["id"],
                        e,
                    )
                break

    current_time = int(datetime.now().timestamp())
    new_videos = []
    for video in video_list:
        vid = video["id"]
        try:
            video_time = int(video.get("createTime", "0"))
        except Exception as e:
            logging.error("Error converting createTime for video %s: %s", vid, e)
            continue
        if video_time <= last_processed_time:
            continue
        if current_time - video_time <= 86400:
            if not should_process_video_group(vid, group_id):
                logging.debug("Skipping duplicate video %s for group %s", vid, group_id)
                continue
            new_videos.append(video)

    logging.debug(
        "Found %d new videos for username %s in group %s", len(new_videos), username, group_id
    )

    topic_id = config.get(group_id, {}).get("topic_id")
    if new_videos:
        new_videos.reverse()  # Process older videos first

        if len(new_videos) > 1:
            media_files = []
            video_ids_batch = []
            for video in new_videos:
                vid = video.get("id")
                if not should_process_video_group(vid, group_id):
                    continue
                mark_video_processed_group(vid, group_id)
                file_path = await safe_download_video(vid, username, uid, group_id=group_id)
                if not file_path:
                    unmark_video_processed_group(vid, group_id)
                    continue
                if isinstance(file_path, list):
                    for single_file in file_path:
                        media_files.append((single_file, vid, "photo"))
                        video_ids_batch.append(vid)
                else:
                    ext = file_path.lower()
                    if ext.endswith((".jpg", ".jpeg", ".png")):
                        media_files.append((file_path, vid, "photo"))
                        video_ids_batch.append(vid)
                    else:
                        media_files.append((file_path, vid, "video"))
                        video_ids_batch.append(vid)

            if media_files:
                for batch_start in range(0, len(media_files), 10):
                    batch = media_files[batch_start : batch_start + 10]
                    media_group = []
                    for file_item in batch:
                        file_path, vid, mtype = file_item
                        if mtype == "video":
                            media_group.append(
                                InputMediaVideo(media=FSInputFile(path=file_path), supports_streaming=True)
                            )
                        else:
                            media_group.append(InputMediaPhoto(media=FSInputFile(file_path)))
                    logging.debug("Sending media group to group %s", group_id)
                    if topic_id:
                        await retry_operation(bot.send_media_group, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            media=media_group,
                            message_thread_id=topic_id,
                        )
                    else:
                        await retry_operation(bot.send_media_group, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            media=media_group,
                        )

                if all(m[2] == "photo" for m in media_files):
                    audio_key = generate_callback_key("audio")
                    AUDIO_CALLBACKS[audio_key] = video_ids_batch
                    # NEW: add Video URLs button
                    video_key = generate_callback_key("video_urls")
                    VIDEO_URL_CALLBACKS[video_key] = video_ids_batch

                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{audio_key}"),
                                InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_key}")
                            ]
                        ]
                    )
                    caption = f"#{username}"
                else:
                    original_url = f"https://www.tiktok.com/@{username}"
                    hd_key = generate_callback_key("hd")
                    audio_key = generate_callback_key("audio")
                    HD_CALLBACKS[hd_key] = video_ids_batch
                    AUDIO_CALLBACKS[audio_key] = video_ids_batch
                    # NEW: add Video URLs button
                    video_key = generate_callback_key("video_urls")
                    VIDEO_URL_CALLBACKS[video_key] = video_ids_batch

                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="Watch Original", url=original_url)],
                            [
                                InlineKeyboardButton(text="HD", callback_data=f"hd_url|{hd_key}"),
                                InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{audio_key}"),
                                InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_key}")
                            ],
                        ]
                    )
                    caption = f"#{username} (total {len(media_files)})"

                if topic_id:
                    await retry_operation(bot.send_message, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        text=caption,
                        reply_markup=keyboard,
                        message_thread_id=topic_id,
                    )
                else:
                    await retry_operation(bot.send_message, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        text=caption,
                        reply_markup=keyboard,
                    )

                for file_item in media_files:
                    file_path = file_item[0]
                    try:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            logging.debug("Deleted file %s after autopost", file_path)
                    except Exception as e:
                        logging.warning("Could not delete %s: %s", file_path, e)

                config[group_id].setdefault("last_downloaded", {})[username] = new_videos[-1].get("id")
                save_autopost_config(config)

        else:
            # Single new video case
            video = new_videos[0]
            video_id = video.get("id")
            mark_video_processed_group(video_id, group_id)
            file_path = await safe_download_video(video_id, username, uid, group_id=group_id)
            if not file_path:
                unmark_video_processed_group(video_id, group_id)
                return
            original_url = f"https://www.tiktok.com/@{username}/video/{video_id}"

            if not isinstance(file_path, list) and file_path.lower().endswith((".jpg", ".jpeg", ".png")):
                audio_key = video_id
                # NEW: use video_id as callback key for single-video case
                VIDEO_URL_CALLBACKS[str(video_id)] = [video_id]

                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{video_id}"),
                            InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_id}")
                        ]
                    ]
                )
                if topic_id:
                    await retry_operation(bot.send_photo, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        photo=FSInputFile(file_path),
                        caption=f"#{username}",
                        reply_markup=keyboard,
                        message_thread_id=topic_id,
                    )
                else:
                    await retry_operation(bot.send_photo, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        photo=FSInputFile(file_path),
                        caption=f"#{username}",
                        reply_markup=keyboard,
                    )

            elif isinstance(file_path, list):
                if len(file_path) == 1:
                    # NEW: use video_id as callback key
                    VIDEO_URL_CALLBACKS[str(video_id)] = [video_id]
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{video_id}"),
                                InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_id}")
                            ]
                        ]
                    )
                    if topic_id:
                        await retry_operation(bot.send_photo, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            photo=FSInputFile(file_path[0]),
                            caption=f"#{username}",
                            reply_markup=keyboard,
                            message_thread_id=topic_id,
                        )
                    else:
                        await retry_operation(bot.send_photo, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            photo=FSInputFile(file_path[0]),
                            caption=f"#{username}",
                            reply_markup=keyboard,
                        )
                else:
                    # Send photo list in batches of up to 10 to avoid "too many messages in album" errors
                    for i in range(0, len(file_path), 10):
                        batch_files = file_path[i : i + 10]
                        media_group = [InputMediaPhoto(media=FSInputFile(p)) for p in batch_files]
                        if topic_id:
                            await retry_operation(bot.send_media_group, attempts=3, delay=1.0)(
                                chat_id=int(group_id),
                                media=media_group,
                                message_thread_id=topic_id,
                            )
                        else:
                            await retry_operation(bot.send_media_group, attempts=3, delay=1.0)(
                                chat_id=int(group_id),
                                media=media_group,
                            )
                        await asyncio.sleep(0.5)

                    audio_key = generate_callback_key("audio")
                    AUDIO_CALLBACKS[audio_key] = [video_id]
                    # NEW: add Video URLs button
                    VIDEO_URL_CALLBACKS[str(video_id)] = [video_id]
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{audio_key}"),
                                InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_id}")
                            ]
                        ]
                    )
                    if topic_id:
                        await retry_operation(bot.send_message, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            text=f"#{username}",
                            reply_markup=keyboard,
                            message_thread_id=topic_id,
                        )
                    else:
                        await retry_operation(bot.send_message, attempts=3, delay=1.0)(
                            chat_id=int(group_id),
                            text=f"#{username}",
                            reply_markup=keyboard,
                        )
            else:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Watch Original Video", url=original_url)],
                        [
                            InlineKeyboardButton(text="HD", callback_data=f"hd_url|{video_id}"),
                            InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{video_id}"),
                            InlineKeyboardButton(text="Video URLs", callback_data=f"video_urls|{video_id}")
                        ],
                    ]
                )
                if topic_id:
                    await retry_operation(bot.send_video, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        video=FSInputFile(path=file_path),
                        caption=f"#{username}",
                        reply_markup=keyboard,
                        message_thread_id=topic_id,
                        supports_streaming=True,
                    )
                else:
                    await retry_operation(bot.send_video, attempts=3, delay=1.0)(
                        chat_id=int(group_id),
                        video=FSInputFile(path=file_path),
                        caption=f"#{username}",
                        reply_markup=keyboard,
                        supports_streaming=True,
                    )

            config[group_id].setdefault("last_downloaded", {})[username] = video_id
            save_autopost_config(config)

            if isinstance(file_path, list):
                for p in file_path:
                    try:
                        if os.path.exists(p):
                            os.remove(p)
                            logging.debug("Deleted file %s after autopost", p)
                    except Exception as e:
                        logging.warning("Could not delete %s: %s", p, e)
            else:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logging.debug("Deleted file %s after autopost", file_path)
                except Exception as e:
                    logging.warning("Could not delete %s: %s", file_path, e)


async def autopost_loop():
    while True:
        try:
            config = load_autopost_config()
            for group_id, data in config.items():
                usernames = data.get("usernames", [])
                for i in range(0, len(usernames), 10):
                    batch = usernames[i : i + 10]
                    for username in batch:
                        await check_updates_for_username(username, group_id, config)
                    await asyncio.sleep(10)
        except Exception as e:
            logging.error("Error in autopost loop: %s", e)
        await asyncio.sleep(200)

# ─── Retry Wrapper ─────────────────────────────────────────────────────────────
def retry_operation(coro, attempts: int = 3, delay: float = 1.0):
    """
    Wraps an async call in up to `attempts` retries with `delay` between them.
    """
    async def _inner(*args, **kwargs):
        for attempt in range(attempts):
            try:
                return await coro(*args, **kwargs)
            except Exception as e:
                logging.error("Attempt %d failed with error: %s", attempt + 1, e)
                if attempt < attempts - 1:
                    await asyncio.sleep(delay)
                else:
                    logging.error("All %d attempts failed.", attempts)
                    return None
    return _inner

# ─── Bot Initialization ───────────────────────────────────────────────────────
bot = Bot(token="7859912037:AAF0qNeTZ57r0zMRHVR7QITjea7wkKL2R7I")  # Replace with your bot token
dp = Dispatcher(storage=MemoryStorage())

# ─── Helper: Check if user is whitelisted ────────────────────────────────────
def is_whitelisted(user_id: int) -> bool:
    return user_id in WHITELISTED_USER_IDS

# ─── Aiogram Command Handlers ─────────────────────────────────────────────────
@dp.message(Command("start"))
async def start_command(message: Message):
    if not is_whitelisted(message.from_user.id):
        await message.reply("Sorry, no access. You are not whitelisted.")
        return
    autopost_data = load_autopost_config()
    user_id_str = str(message.from_user.id)
    if user_id_str not in autopost_data:
        autopost_data[user_id_str] = {"usernames": [], "last_downloaded": {}}
        save_autopost_config(autopost_data)
    welcome_message = (
        "✅ <b>Welcome!</b> You have access to the bot.\n\n"
        "Here are some commands to get you started:\n\n"
        "• <b>/ap</b> - Set one or more usernames for autopost. For example:\n"
        "   <code>/ap user1,user2:-1002256109215:3</code>\n"
        "   Or <code>/ap user1,user2 -1002256109215 3</code>.\n"
        "   This sets autopost for the given usernames in the specified group and topic.\n\n"
        "• <b>/remove_autopost</b> - Remove a username or an entire autopost configuration.\n"
        "   Use it like <code>/remove_autopost -1002256109215 user1</code>.\n\n"
        "• <b>/cap</b> - Check your current autopost configuration\n\n"
        "Use these commands to manage your autopost settings for receiving new TikTok updates."
    )
    await message.reply(welcome_message, disable_web_page_preview=True, parse_mode="HTML")

@dp.message(Command("whitelist"))
async def bulk_whitelist(message: Message):
    """
    Command to bulk-add Telegram user IDs to the whitelist.
    Usage: /whitelist <id1>,<id2>,<id3>,...
    Only existing whitelisted users can invoke this.
    """
    if not is_whitelisted(message.from_user.id):
        await message.reply("❌ You do not have permission to modify the whitelist.")
        return

    # Split off the command itself
    parts = message.text.split(None, 1)
    if len(parts) != 2:
        await message.reply("Usage: /whitelist <user_id1>,<user_id2>,<user_id3> ...")
        return

    id_list_str = parts[1]
    # Split by comma, strip whitespace
    raw_ids = [s.strip() for s in id_list_str.split(",") if s.strip()]
    added = []
    already_whitelisted = []
    invalid = []

    for id_str in raw_ids:
        try:
            uid = int(id_str)
        except ValueError:
            invalid.append(id_str)
            continue
        if uid in WHITELISTED_USER_IDS:
            already_whitelisted.append(uid)
        else:
            WHITELISTED_USER_IDS.add(uid)
            added.append(uid)

    save_whitelist(WHITELISTED_USER_IDS)

    reply_parts = []
    if added:
        reply_parts.append(f"✅ Added to whitelist: {', '.join(str(x) for x in added)}")
    if already_whitelisted:
        reply_parts.append(f"ℹ️ Already whitelisted: {', '.join(str(x) for x in already_whitelisted)}")
    if invalid:
        reply_parts.append(f"❌ Invalid IDs skipped: {', '.join(invalid)}")

    await message.reply("\n".join(reply_parts), parse_mode="Markdown")

@dp.message(Command("remove_whitelist"))
async def remove_whitelist_command(message: Message):
    """
    Command to remove a Telegram user ID from the whitelist.
    Usage: /remove_whitelist <user_id>
    Only existing whitelisted users can remove IDs.
    """
    if not is_whitelisted(message.from_user.id):
        await message.reply("❌ You do not have permission to modify the whitelist.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Usage: /remove_whitelist <user_id>")
        return

    try:
        rem_user_id = int(parts[1])
    except ValueError:
        await message.reply("Invalid user ID. It must be an integer.")
        return

    if rem_user_id not in WHITELISTED_USER_IDS:
        await message.reply(f"User `{rem_user_id}` is not in the whitelist.", parse_mode="Markdown")
        return

    WHITELISTED_USER_IDS.remove(rem_user_id)
    save_whitelist(WHITELISTED_USER_IDS)
    await message.reply(f"✅ User `{rem_user_id}` has been removed from the whitelist.", parse_mode="Markdown")
    logging.debug("Removed user %s from whitelist", rem_user_id)

@dp.message(Command("cap"))
async def check_autopost(message: Message):
    if not is_whitelisted(message.from_user.id):
        await message.reply("Sorry, no access. You are not whitelisted.")
        return

    config = load_autopost_config()

    # Determine group_id: if in private chat and no argument, use the user’s own ID
    if message.chat.type == "private":
        args = message.text.split()[1:]
        if not args:
            group_id_str = str(message.from_user.id)
        else:
            group_id_str = args[0]
    else:
        group_id_str = str(message.chat.id)

    if group_id_str in config:
        usernames = config[group_id_str]["usernames"]
        active_autopost = ", ".join(f"`{u}`" for u in usernames)
        header = f"Active autoposts for `{group_id_str}`:\n"
        full_text = header + active_autopost
        MAX_MESSAGE_LENGTH = 4096
        if len(full_text) <= MAX_MESSAGE_LENGTH:
            await message.reply(full_text, parse_mode="Markdown")
        else:
            usernames_list = active_autopost.split(", ")
            current_chunk = header
            for username in usernames_list:
                addition = f"{username}, "
                if len(current_chunk) + len(addition) > MAX_MESSAGE_LENGTH:
                    await message.reply(current_chunk.rstrip(", "), parse_mode="Markdown")
                    current_chunk = ""
                current_chunk += addition
            if current_chunk:
                await message.reply(current_chunk.rstrip(", "), parse_mode="Markdown")
    else:
        await message.reply(f"No autopost configuration found for `{group_id_str}`.")


@dp.message(Command("rp"))
async def remove_posting_users(message: Message):
    # Only allow whitelisted users
    if not is_whitelisted(message.from_user.id):
        await message.reply("🚫 You are not whitelisted.")
        return

    # Split off the comma-list of usernames
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(
            "**Usage:** `/rp username1,username2,...`",
            parse_mode="Markdown",
        )
        return

    # Always use sender’s user ID as the config key
    group_id = str(message.from_user.id)

    # Parse usernames to remove
    to_remove = [u.strip() for u in parts[1].split(",") if u.strip()]
    if not to_remove:
        await message.reply("*No valid usernames provided to remove.*", parse_mode="Markdown")
        return

    # Load config
    config = load_autopost_config()
    if group_id not in config:
        await message.reply(
            f"No autopost configuration found for your user ID `{group_id}`.",
            parse_mode="Markdown",
        )
        return

    existing = set(config[group_id].get("usernames", []))
    removed = []
    not_found = []

    # Remove each username if present
    for uname in to_remove:
        if uname in existing:
            existing.remove(uname)
            removed.append(uname)
            # Also drop from last_downloaded if present
            config[group_id].get("last_downloaded", {}).pop(uname, None)
        else:
            not_found.append(uname)

    # Update config
    config[group_id]["usernames"] = list(existing)
    save_autopost_config(config)

    # Build reply
    reply_lines = []
    if removed:
        reply_lines.append(f"✅ Removed: {', '.join(f'`{u}`' for u in removed)}")
    if not_found:
        reply_lines.append(f"⚠️ Not found: {', '.join(f'`{u}`' for u in not_found)}")

    await message.reply("\n".join(reply_lines), parse_mode="Markdown")
    logging.debug(
        "Autopost for user %s updated. Removed: %s; Not found: %s",
        group_id,
        removed,
        not_found,
    )



@dp.message(Command("ap"))
async def autopost_command(message: Message):
    # Only allow whitelisted users
    if not is_whitelisted(message.from_user.id):
        await message.reply("🚫 You are not whitelisted.")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply(
            "**Usage:** `/ap username1,username2,...`",
            parse_mode="Markdown",
        )
        return

    # Use sender’s user ID as the config key
    group_id = str(message.from_user.id)

    # Parse and validate usernames
    usernames_part = parts[1]
    new_usernames = [u.strip() for u in usernames_part.split(",") if u.strip()]
    if not new_usernames:
        await message.reply("*No valid usernames provided.*", parse_mode="Markdown")
        return

    # Load existing config and merge or create new entry
    config = load_autopost_config()
    if group_id in config:
        existing = set(config[group_id].get("usernames", []))
        merged = list(existing.union(new_usernames))
        config[group_id]["usernames"] = merged
        config[group_id].setdefault("last_downloaded", {})
        for uname in merged:
            config[group_id]["last_downloaded"].setdefault(uname, "")
    else:
        config[group_id] = {
            "usernames": new_usernames,
            "last_downloaded": {u: "" for u in new_usernames},
        }

    save_autopost_config(config)

    # Send confirmation
    formatted = ", ".join(f"`{u}`" for u in new_usernames)
    await message.reply(
        f"✅ **Autopost configured**\n"
        f"👤 **Your ID:** `{group_id}`\n"
        f"👥 **Usernames:** {formatted}",
        parse_mode="Markdown",
    )

    logging.debug(
        "Configured autopost for user %s: %s",
        group_id,
        config[group_id]["usernames"],
    )

@dp.message(Command("post"))
async def post_video(message: Message):
    if not is_whitelisted(message.from_user.id):
        await message.reply("Sorry, no access. You are not whitelisted.")
        return

    parts = message.text.split()
    if len(parts) < 3:
        await message.reply("Usage: /post <video_url> <group_id_or_link> [<topic_id>]")
        return

    input_url = parts[1]
    group_input = parts[2]
    topic_id = parts[3] if len(parts) >= 4 else None
    resolved_url = input_url

    # Convert "t.me/c/XXX/YY" into a valid chat_id
    private_group_match = re.match(r"https://t\.me/c/(\d+)/\d+", group_input)
    if private_group_match:
        extracted_id = private_group_match.group(1)
        group_id = f"-100{extracted_id}"
    else:
        group_id = group_input

    # Extract the video ID from the URL
    video_id_match = re.search(r"/video/(\d+)", input_url)
    if not video_id_match:
        try:
            resolved = requests.get(input_url, allow_redirects=True, timeout=10)
            resolved_url = resolved.url
            video_id_match = re.search(r"/video/(\d+)", resolved_url)
            if not video_id_match:
                await message.reply("Invalid TikTok video URL after resolving.")
                return
        except Exception as e:
            await message.reply(f"Error resolving URL: {e}")
            return

    video_id = video_id_match.group(1)

    # Extract username from the resolved URL
    username_match = re.search(r"@([^/]+)/video/", resolved_url)
    username = username_match.group(1) if username_match else "Unknown"

    direct_video_url = f"https://www.tikwm.com/video/media/play/{video_id}.mp4"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Watch Original Video", url=input_url)],
            [
                InlineKeyboardButton(text="HD", callback_data=f"hd_url|{video_id}"),
                InlineKeyboardButton(text="Audio", callback_data=f"audio_url|{video_id}"),
            ],
        ]
    )

    await message.reply(f"Posting video to {group_id}{f' on topic {topic_id}' if topic_id else ''}...")
    try:
        if topic_id:
            await bot.send_video(
                chat_id=int(group_id),
                video=direct_video_url,
                caption=f"#{username}",
                reply_markup=keyboard,
                message_thread_id=int(topic_id),
            )
        else:
            await bot.send_video(
                chat_id=int(group_id),
                video=direct_video_url,
                caption=f"#{username}",
                reply_markup=keyboard,
            )
        logging.debug(
            "Posted video %s (by @%s) to group %s%s using URL: %s",
            video_id,
            username,
            group_id,
            f" on topic {topic_id}" if topic_id else "",
            direct_video_url,
        )
    except Exception as e:
        logging.error("Error posting video to group %s: %s", group_id, e)
        await message.reply(f"Error posting video: {e}")

# ─── Callback Queries for HD and Audio buttons ─────────────────────────────────

# New callback handler for Video URLs button
@dp.callback_query(lambda call: call.data.startswith("video_urls|"))
async def on_video_urls_callback(call):
    _, key = call.data.split("|", 1)
    vids = VIDEO_URL_CALLBACKS.get(key, [])
    if not vids:
        return await call.answer("No URLs available.", show_alert=True)

    # Extract username from the caption text
    username = call.message.text.lstrip("#").split()[0]
    for vid_id in vids:
        url = f"https://www.tiktok.com/@{username}/video/{vid_id}"
        await bot.send_message(chat_id=call.from_user.id, text=url)

    await call.answer("Sent you the video URLs via DM!", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith("hd_url|"))
async def send_hd_video(callback_query: CallbackQuery):
    data = callback_query.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        await callback_query.answer("Invalid callback data", show_alert=True)
        return

    key_or_id = parts[1]
    if key_or_id in HD_CALLBACKS:
        video_ids = HD_CALLBACKS.pop(key_or_id)
    else:
        video_ids = [key_or_id]

    await callback_query.answer(f"Dm'ing you {len(video_ids)} HD video(s)", show_alert=False)
    hd_files = []
    for vid in video_ids:
        if vid in HD_URLS:
            hd_url = HD_URLS[vid]
        else:
            hd_url = f"https://www.tikwm.com/video/media/hdplay/{vid}.mp4"
            HD_URLS[vid] = hd_url
            save_hd_urls(HD_URLS)
        try:
            response = requests.get(hd_url, stream=True, timeout=TIMEOUT)
            if response.status_code == 200:
                temp_file = f"HD_{vid}_{random.randint(1000,9999)}.mp4"
                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                hd_files.append(temp_file)
            else:
                await callback_query.answer("Failed to retrieve HD video.", show_alert=True)
        except Exception as e:
            await callback_query.answer(f"Error: {e}", show_alert=True)

    for file in hd_files:
        video_file = FSInputFile(path=file)
        await retry_operation(bot.send_document, attempts=3, delay=1.0)(
            callback_query.from_user.id, document=video_file, disable_content_type_detection=True
        )
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception as e:
            logging.warning("Could not delete temp file %s: %s", file, e)

@dp.callback_query(lambda c: c.data and c.data.startswith("audio_url|"))
async def send_audio_file(callback_query: CallbackQuery):
    data = callback_query.data
    parts = data.split("|", 1)
    if len(parts) != 2:
        await callback_query.answer("Invalid callback data", show_alert=True)
        return

    key_or_id = parts[1]
    if key_or_id in AUDIO_CALLBACKS:
        video_ids = AUDIO_CALLBACKS.pop(key_or_id)
    else:
        video_ids = [key_or_id]

    await callback_query.answer(f"Dm'ing you {len(video_ids)} audio file(s)", show_alert=False)
    for vid in video_ids:
        audio_url = f"https://www.tikwm.com/video/music/{vid}.mp3"
        try:
            response = requests.get(audio_url, stream=True, timeout=TIMEOUT)
            if response.status_code == 200:
                temp_file = f"Audio_{vid}_{random.randint(1000,9999)}.mp3"
                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                audio_file = FSInputFile(temp_file)
                await retry_operation(bot.send_document, attempts=3, delay=1.0)(
                    callback_query.from_user.id, document=audio_file, disable_content_type_detection=True
                )
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except Exception as e:
                    logging.warning("Could not delete temp file %s: %s", temp_file, e)
            else:
                await callback_query.answer("Failed to retrieve audio.", show_alert=True)
        except Exception as e:
            await callback_query.answer(f"Error: {e}", show_alert=True)

# ─── Set Bot Commands ──────────────────────────────────────────────────────────
async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="ap", description="Enable autopost (usage: /ap user1,user2 <group_id[:topic_id]>)"),
        BotCommand(command="cap", description="Check your autopost configuration"),
        BotCommand(command="rp", description="Remove a user from autopost (usage: /rp <group_id> <username>)"),
    ]
    await bot.set_my_commands(commands)
    logging.debug("Bot commands set.")

# ─── Main Entry Point ─────────────────────────────────────────────────────────
async def main():
    await set_bot_commands()
    asyncio.create_task(autopost_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
