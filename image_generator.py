#!/usr/bin/env python3
"""
Dynamic image generator for deal room profile pictures
Adds room number to template image
"""

from PIL import Image, ImageDraw, ImageFont
import os

def generate_room_image(room_number, output_path="room_profile.jpg"):
    """Generate a room profile picture by adding room number to template"""
    
    template_path = "template.jpg"
    
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template image not found: {template_path}")
    
    # Open template image
    img = Image.open(template_path).convert('RGB')
    draw = ImageDraw.Draw(img)
    
    # Load font for room number text - using exact same Bold weight as ROOM text
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
    except:
        font = ImageFont.load_default()
    
    # Text to add (space before number to separate from "ROOM")
    text = f" {room_number}"
    text_color = (255, 255, 255)  # White
    
    # Position based on comparing example "ROOM 21" with generated "ROOM 41"
    # Adjusted gap and size to match example image
    text_x = 410
    text_y = 280
    
    # Draw text
    draw.text((text_x, text_y), text, fill=text_color, font=font)
    
    # Save the image
    img.save(output_path, quality=95)
    return output_path


if __name__ == "__main__":
    # Test the image generator
    for room_num in [40, 41, 43]:
        path = generate_room_image(room_num, f"room_{room_num}.jpg")
        print(f"Generated: {path}")
