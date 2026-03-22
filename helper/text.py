import numpy as np
import concurrent.futures
import os
import time
import logging
from moviepy import *
from moviepy.video.fx import FadeIn, FadeOut
from moviepy.video.fx import CrossFadeOut, CrossFadeIn
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from helper.minor_helper import measure_time
from helper.shorts_assets import get_default_font_path
from functools import partial

try:
    import dill
    HAS_DILL = True
except ImportError:
    HAS_DILL = False

logger = logging.getLogger(__name__)


def _process_text_section_standalone(section, helper_resolution, helper_body_font_path, create_text_clip_func,
                                     animation="fade", font_size=60, font_path=None, with_pill=False, position='center'):
    try:
        text = section.get('text', '')
        duration = section.get('duration', 5)
        section_position = section.get('position', position)
        section_font_size = section.get('font_size', font_size)

        if not font_path:
            font_path = helper_body_font_path

        return create_text_clip_func(
            text=text,
            duration=duration,
            font_size=section_font_size,
            font_path=font_path,
            position=section_position,
            animation=animation,
            with_pill=with_pill,
        )
    except Exception as e:
        logger.error(f"Error creating text clip: {e}")
        return None


class TextHelper:
    def __init__(self):
        self.resolution = (1080, 1920)
        self.fonts_dir = os.path.join(os.path.dirname(__file__), 'fonts')
        os.makedirs(self.fonts_dir, exist_ok=True)
        default_font_path = get_default_font_path()
        self.title_font_path = default_font_path
        self.body_font_path = default_font_path
        self.subtitle_font_size = max(int(os.getenv("SHORTS_SUBTITLE_FONT_SIZE", "72")), 48)
        self.subtitle_y_ratio = float(os.getenv("SHORTS_SUBTITLE_Y_RATIO", "0.42"))
        self.subtitle_position = ("center", int(self.resolution[1] * self.subtitle_y_ratio))

        self.transitions = {
            "fade": lambda clip, duration: clip.with_effects([FadeIn(duration)]),
            "fade_out": lambda clip, duration: clip.with_effects([FadeOut(duration)]),
            "slide": lambda clip, duration: clip.with_position(lambda t: (0, 0 + t * (self.resolution[1] / duration))),
            "slide_out": lambda clip, duration: clip.with_position(lambda t: (0, self.resolution[1] - t * (self.resolution[1] / duration))),
            "zoom": lambda clip, duration: clip.resized(lambda t: 1 + t * (0.5 / duration)),
            "zoom_out": lambda clip, duration: clip.resized(lambda t: 1 - t * (0.5 / duration)),
        }

    @measure_time
    def _create_pill_image(self, size, color=(0, 0, 0, 160), radius=30):
        width, height = size
        img = Image.new('RGBA', (width, height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        draw.rectangle([(radius, 0), (width - radius, height)], fill=color)
        draw.rectangle([(0, radius), (width, height - radius)], fill=color)
        draw.ellipse([(0, 0), (radius * 2, radius * 2)], fill=color)
        draw.ellipse([(width - radius * 2, 0), (width, radius * 2)], fill=color)
        draw.ellipse([(0, height - radius * 2), (radius * 2, height)], fill=color)
        draw.ellipse([(width - radius * 2, height - radius * 2), (width, height)], fill=color)

        return img

    @measure_time
    def _create_text_clip(self, text, duration=5, font_size=None, font_path=None, color='white',
                          position=None, animation="fade", animation_duration=1.0, shadow=True,
                          outline=False, with_pill=False, pill_color=(0, 0, 0, 160), pill_radius=30):
        logger.info(f"Creating text clip: '{text[:30]}...' with duration {duration:.2f}s")

        if font_size is None:
            font_size = self.subtitle_font_size

        if not font_path:
            font_path = self.body_font_path

        if position is None:
            position = self.subtitle_position

        resolved_font_path = font_path
        try:
            text_clip = TextClip(
                text=text,
                font=font_path,
                font_size=font_size,
                color=color,
                method='caption',
                size=(self.resolution[0] - 100, None)
            )
        except Exception:
            resolved_font_path = ""
            text_clip = TextClip(
                text=text,
                font_size=font_size,
                font="",
                color=color,
                method='caption',
                size=(self.resolution[0] - 100, None)
            )

        text_clip = text_clip.with_duration(duration)
        clips = []

        if with_pill:
            pill_image = self._create_pill_image(text_clip.size, color=pill_color, radius=pill_radius)
            pill_clip = ImageClip(np.array(pill_image), duration=duration)
            clips.append(pill_clip)

        if shadow:
            try:
                shadow_clip = TextClip(
                    text=text,
                    font=resolved_font_path,
                    font_size=font_size,
                    color='black',
                    method='caption',
                    size=(self.resolution[0] - 100, None)
                ).with_position((5, 5), relative=True).with_opacity(0.8).with_duration(duration)

                def _blur_frame(frame):
                    try:
                        pil = Image.fromarray(frame)
                        blurred = pil.filter(ImageFilter.GaussianBlur(radius=3))
                        return np.array(blurred)
                    except Exception:
                        return frame

                shadow_clip = shadow_clip.fl_image(_blur_frame)
                clips.append(shadow_clip)
            except Exception as e:
                logger.warning(f"Shadow text rendering failed: {e}")

        if outline:
            outline_clips = []
            for dx, dy in [(-1, -1), (-1, 1), (1, -1), (1, 1)]:
                try:
                    oc = TextClip(
                        text=text,
                        font=resolved_font_path,
                        font_size=font_size,
                        color='black',
                        method='caption',
                        size=(self.resolution[0] - 100, None)
                    ).with_position((dx, dy), relative=True).with_opacity(0.5).with_duration(duration)
                    outline_clips.append(oc)
                except Exception as e:
                    logger.warning(f"Outline text rendering failed: {e}")
            clips.extend(outline_clips)

        clips.append(text_clip)
        text_composite = CompositeVideoClip(clips)
        text_composite = text_composite.with_position(position)

        if animation in self.transitions:
            anim_func = self.transitions[animation]
            text_composite = anim_func(text_composite, animation_duration)

        final_clip = text_composite.with_duration(duration)
        logger.info(f"Created text clip with final duration: {final_clip.duration:.2f}s")

        return final_clip

    def _process_text_section(self, section, animation="fade", font_size=60, font_path=None, with_pill=False, position='center'):
        try:
            text = section.get('text', '')
            duration = section.get('duration', 5)
            section_idx = section.get('section_idx', -1)
            section_position = section.get('position', position)
            section_font_size = section.get('font_size', font_size)

            logger.info(f"Processing text section {section_idx}: '{text[:30]}...' duration={duration:.2f}s")

            result = self._create_text_clip(
                text=text,
                duration=duration,
                font_size=section_font_size,
                font_path=font_path,
                position=section_position,
                animation=animation,
                with_pill=with_pill,
            )

            if result:
                result._section_idx = section_idx
                result._debug_info = f"Text section {section_idx}"
                logger.info(f"Created text clip for section {section_idx} with duration {result.duration:.2f}s")

            return result
        except Exception as e:
            logger.error(f"Error creating text clip for section {section.get('section_idx', -1)}: {e}")
            return None

    @measure_time
    def generate_text_clips_parallel(self, script_sections, max_workers=None,
                                     animation="fade", font_size=None, font_path=None,
                                     with_pill=False, position=None):
        start_time = time.time()
        logger.info(f"Generating {len(script_sections)} text clips in parallel")

        if not script_sections:
            return []

        if not max_workers:
            max_workers = max(1, min(len(script_sections), os.cpu_count() or 1))

        text_clips = [None] * len(script_sections)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_text_section,
                    section,
                    animation=animation,
                    font_size=font_size,
                    font_path=font_path,
                    with_pill=with_pill,
                    position=position,
                ): idx
                for idx, section in enumerate(script_sections)
            }

            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    text_clips[idx] = future.result()
                except Exception as e:
                    logger.error(f"Error in parallel text clip generation for section {idx}: {e}")

        text_clips = [clip for clip in text_clips if clip is not None]
        total_time = time.time() - start_time
        logger.info(f"Generated {len(text_clips)} text clips in {total_time:.2f} seconds")
        return text_clips

    @measure_time
    def _create_word_by_word_clip(self, text, duration, font_size=None, font_path=None,
                                   text_color=(255, 255, 255, 255),
                                   pill_color=(0, 0, 0, 160),
                                   position=None):
        logger.info(f"Creating word-by-word clip: '{text[:30]}...' with duration {duration:.2f}s")

        if font_size is None:
            font_size = self.subtitle_font_size

        if not font_path:
            font_path = self.body_font_path

        if position is None:
            position = self.subtitle_position

        if not text.strip():
            bg = ColorClip(size=self.resolution, color=(0, 0, 0, 0)).with_duration(duration)
            return bg

        words = text.split()
        word_count = len(words)
        char_counts = [len(w) for w in words]
        total_chars = sum(char_counts)

        min_word_time = 0.4
        if total_chars > 0:
            effective_duration = duration * 0.85
            time_per_char = effective_duration / total_chars
            word_durations = [max(min_word_time, len(w) * time_per_char) for w in words]
        else:
            word_durations = [duration]

        total_word_duration = sum(word_durations)
        remaining_time = duration - total_word_duration
        transition_duration = max(0.15, remaining_time / max(1, word_count - 1)) if word_count > 1 else 0

        adjusted_total = sum(word_durations) + transition_duration * max(0, word_count - 1)
        if abs(adjusted_total - duration) > 0.01 and word_count > 0:
            adjustment_factor = (duration - transition_duration * max(0, word_count - 1)) / sum(word_durations)
            word_durations = [d * adjustment_factor for d in word_durations]

        clips = []
        for word, word_duration in zip(words, word_durations):
            try:
                font = ImageFont.truetype(font_path, font_size)
            except Exception:
                font = ImageFont.load_default()

            dummy_img = Image.new('RGBA', (1, 1))
            dummy_draw = ImageDraw.Draw(dummy_img)
            text_bbox = dummy_draw.textbbox((0, 0), word, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            padding_x = int(font_size * 0.7)
            padding_y = int(font_size * 0.35)
            img_width = text_width + padding_x * 2
            img_height = text_height + padding_y * 2

            img = Image.new('RGBA', (img_width, img_height), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            radius = min(img_height // 2, padding_y + padding_y // 2)
            draw.rectangle([(radius, 0), (img_width - radius, img_height)], fill=pill_color)
            draw.rectangle([(0, radius), (img_width, img_height - radius)], fill=pill_color)
            draw.ellipse([(0, 0), (radius * 2, radius * 2)], fill=pill_color)
            draw.ellipse([(img_width - radius * 2, 0), (img_width, radius * 2)], fill=pill_color)
            draw.ellipse([(0, img_height - radius * 2), (radius * 2, img_height)], fill=pill_color)
            draw.ellipse([(img_width - radius * 2, img_height - radius * 2), (img_width, img_height)], fill=pill_color)

            ascent, descent = font.getmetrics() if hasattr(font, 'getmetrics') else (0, 0)
            text_x = (img_width - text_width) // 2
            vertical_offset = (descent - ascent) // 4 if ascent or descent else 0
            text_y = (img_height - text_height) // 2 + vertical_offset

            draw.text((text_x, text_y), word, font=font, fill=text_color)
            word_image = img
            word_clip = ImageClip(np.array(word_image), duration=word_duration)
            clips.append(word_clip)

        if len(clips) == 1:
            word_sequence = clips[0]
        else:
            try:
                concatenated_clips = []
                for i, clip in enumerate(clips):
                    if i > 0:
                        clip = clip.with_effects((CrossFadeIn(transition_duration/2)))
                    if i < len(clips) - 1:
                        clip = clip.with_effects((CrossFadeOut(transition_duration/2)))
                    concatenated_clips.append(clip)
                word_sequence = concatenate_videoclips(concatenated_clips, method="compose")
            except Exception:
                concatenated_clips = []
                for i, clip in enumerate(clips):
                    if i > 0:
                        clip = clip.with_effects([FadeIn(transition_duration/2)])
                    if i < len(clips) - 1:
                        clip = clip.with_effects([FadeOut(transition_duration/2)])
                    concatenated_clips.append(clip)
                word_sequence = concatenate_videoclips(concatenated_clips, method="compose")

        bg = ColorClip(size=self.resolution, color=(0, 0, 0, 0)).with_duration(word_sequence.duration)
        positioned_sequence = word_sequence.with_position(position)
        final_clip = CompositeVideoClip([bg, positioned_sequence], size=self.resolution)
        final_clip = final_clip.with_duration(duration)
        logger.info(f"Created word-by-word clip with final duration: {final_clip.duration:.2f}s")
        return final_clip

    def _process_word_by_word_section(self, section, font_size=None, font_path=None):
        try:
            text = section.get('text', '')
            duration = section.get('duration', 5)
            section_idx = section.get('section_idx', -1)
            position = section.get('position', self.subtitle_position)
            section_font_size = section.get('font_size', font_size)

            result = self._create_word_by_word_clip(
                text=text,
                duration=duration,
                font_size=section_font_size,
                font_path=font_path,
                position=position,
            )
            if result:
                result._section_idx = section_idx
                result._debug_info = f"Word-by-word section {section_idx}"
            return result
        except Exception as e:
            logger.error(f"Error creating word-by-word clip for section {section.get('section_idx', -1)}: {e}")
            return None

    @measure_time
    def generate_word_by_word_clips_parallel(self, script_sections, max_workers=None,
                                             font_size=None, font_path=None):
        start_time = time.time()
        logger.info(f"Generating {len(script_sections)} word-by-word text clips in parallel")
        if not max_workers:
            max_workers = min(len(script_sections), os.cpu_count() or 1)

        results_with_index = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, section in enumerate(script_sections):
                futures[executor.submit(self._process_word_by_word_section, section, font_size, font_path)] = i
            for future in concurrent.futures.as_completed(futures):
                section_idx = futures[future]
                try:
                    clip = future.result()
                    if clip is not None:
                        clip._section_idx = section_idx
                        clip._debug_info = f"Word-by-word clip {section_idx}"
                        results_with_index.append((section_idx, clip))
                except Exception as e:
                    logger.error(f"Error in parallel word-by-word clip generation for section {section_idx}: {e}")

        results_with_index.sort(key=lambda x: x[0])
        text_clips = [clip for _, clip in results_with_index]
        total_time = time.time() - start_time
        logger.info(f"Generated {len(text_clips)} word-by-word text clips in {total_time:.2f} seconds")
        return text_clips

    @measure_time
    def add_watermark(self, clip, watermark_text="Lazycreator", position=("right", "top"), opacity=0.7, font_size=30):
        txt_clip = TextClip(watermark_text, font=self.body_font_path, font_size=font_size, color='white',
                            stroke_color='gray', stroke_width=1)
        txt_clip = txt_clip.with_duration(clip.duration).with_opacity(opacity)

        margin_x = int(self.resolution[0] * 0.03)
        margin_y = int(self.resolution[1] * 0.02)

        if position[0] == "left":
            x_pos = margin_x
        elif position[0] == "right":
            x_pos = self.resolution[0] - txt_clip.w - margin_x
        else:
            x_pos = (self.resolution[0] - txt_clip.w) // 2

        if position[1] == "top":
            y_pos = margin_y
        elif position[1] == "bottom":
            y_pos = self.resolution[1] - txt_clip.h - margin_y
        else:
            y_pos = (self.resolution[1] - txt_clip.h) // 2

        return CompositeVideoClip([clip, txt_clip.with_position((x_pos, y_pos))])
