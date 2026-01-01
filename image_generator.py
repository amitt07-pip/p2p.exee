#!/usr/bin/env python3
"""
Dynamic image generator for deal room profile pictures
Adds room number to template image
"""

from PIL import Image, ImageDraw, ImageFont
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def generate_room_image(room_number, output_path="room_profile.jpg"):
    """Generate a room profile picture by adding room number to template"""
    
    template_path = os.path.join(SCRIPT_DIR, "template.jpg")
    
    if not os.path.exists(template_path):
        old_template = "attached_assets/photo_4980985509068868445_x_1763963636510.jpg"
        if os.path.exists(old_template):
            template_path = old_template
        else:
            raise FileNotFoundError(f"Template image not found: {template_path}")
    
    img = Image.open(template_path).convert('RGB')
    draw = ImageDraw.Draw(img)
    
    poppins_font_path = os.path.join(SCRIPT_DIR, "fonts", "Poppins-Bold.ttf")
    try:
        if os.path.exists(poppins_font_path):
            font = ImageFont.truetype(poppins_font_path, 58)
        else:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 58)
    except:
        font = ImageFont.load_default()
    
    text = f" {room_number}"
    text_color = (255, 255, 255)
    
    text_x = 415
    text_y = 280
    
    draw.text((text_x, text_y), text, fill=text_color, font=font)
    
    img.save(output_path, quality=95)
    return output_path


if __name__ == "__main__":
    # Test the image generator
    for room_num in [40, 41, 43]:
        path = generate_room_image(room_num, f"room_{room_num}.jpg")
        print(f"Generated: {path}")
